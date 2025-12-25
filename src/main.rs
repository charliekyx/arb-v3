use anyhow::{anyhow, Context, Result};
use chrono::Local;
use cocoon::Cocoon;
use ethers::{
    prelude::*,
    types::{Address, Eip1559TransactionRequest, U256},
    utils::{format_ether, parse_ether, parse_units},
};
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    env,
    fs::{self, File},
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tracing::{error, info, warn};

// --- Config Structs ---
#[derive(Serialize, Deserialize, Debug, Clone)]
struct AppConfig {
    private_key: String,
    ipc_path: String,
    contract_address: String,
    // 保留字段定义以兼容配置文件，但代码中不使用
    smtp_username: String,
    smtp_password: String,
    my_email: String,
}

#[derive(Debug, Deserialize, Clone)]
struct JsonPoolInput {
    name: String,
    token_a: String,
    token_b: String,
    router: String,
    quoter: String,
    fee: u32,
}

#[derive(Clone, Debug)]
struct PoolConfig {
    name: String,
    router: Address,
    quoter: Address,
    fee: u32,
    token_other: Address,
}

// --- ABI ---
abigen!(
    FlashLoanExecutor,
    r#"[
        struct SwapStep { address router; address tokenIn; address tokenOut; uint24 fee; }
        function executeArb(uint256 borrowAmount, SwapStep[] steps, uint256 minProfit) external
    ]"#;

    IQuoterV2,
    r#"[
        struct QuoteParams { address tokenIn; address tokenOut; uint256 amountIn; uint24 fee; uint160 sqrtPriceLimitX96; }
        function quoteExactInputSingle(QuoteParams params) external returns (uint256 amountOut, uint160 sqrtPriceX96After, uint32 initializedTicksCrossed, uint256 gasEstimate)
    ]"#
);

const WETH_ADDR: &str = "0x4200000000000000000000000000000000000006";
const MAX_DAILY_GAS_LOSS_WEI: u128 = 20_000_000_000_000_000; // 0.02 ETH

// --- Helpers ---
#[derive(Serialize, Deserialize, Debug, Default)]
struct GasState {
    date: String,
    accumulated_loss: u128,
}

struct SharedGasManager {
    accumulated_loss: Mutex<u128>,
}

impl SharedGasManager {
    fn new(path: String) -> Self {
        let loaded = Self::load_gas_state(&path);
        Self {
            accumulated_loss: Mutex::new(loaded.accumulated_loss),
        }
    }
    fn load_gas_state(path: &str) -> GasState {
        let today = Local::now().format("%Y-%m-%d").to_string();
        if let Ok(c) = fs::read_to_string(path) {
            if let Ok(s) = serde_json::from_str::<GasState>(&c) {
                if s.date == today {
                    return s;
                }
            }
        }
        GasState {
            date: today,
            accumulated_loss: 0,
        }
    }
    fn get_loss(&self) -> u128 {
        *self.accumulated_loss.lock().unwrap()
    }
}

struct NonceManager {
    nonce: AtomicU64,
    provider: Arc<Provider<Ipc>>,
    address: Address,
}

impl NonceManager {
    async fn new(provider: Arc<Provider<Ipc>>, address: Address) -> Result<Self> {
        let start_nonce = provider.get_transaction_count(address, None).await?;
        Ok(Self {
            nonce: AtomicU64::new(start_nonce.as_u64()),
            provider,
            address,
        })
    }
    fn get_next(&self) -> U256 {
        U256::from(self.nonce.fetch_add(1, Ordering::SeqCst))
    }
    async fn sync_from_chain(&self) -> Result<()> {
        let on_chain = self
            .provider
            .get_transaction_count(self.address, None)
            .await?;
        self.nonce.store(on_chain.as_u64(), Ordering::SeqCst);
        Ok(())
    }
}

// --- Main ---

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    info!("🚀 System Starting: Base V3 Concurrent Bot (Clean Version)");

    // 1. Config
    let config = load_encrypted_config()?;
    let provider = Arc::new(Provider::<Ipc>::connect_ipc(&config.ipc_path).await?);
    let wallet = LocalWallet::from_str(&config.private_key)?.with_chain_id(8453u64);
    let my_addr = wallet.address();
    let client = Arc::new(SignerMiddleware::new(provider.clone(), wallet.clone()));

    let contract_addr: Address = config.contract_address.parse()?;
    let executor_contract = FlashLoanExecutor::new(contract_addr, client.clone());
    let gas_manager = Arc::new(SharedGasManager::new("gas_state.json".to_string()));
    let nonce_manager = Arc::new(NonceManager::new(provider.clone(), my_addr).await?);

    // 2. Load Pools
    let config_content = fs::read_to_string("pools.json").context("Failed to read pools.json")?;
    let json_configs: Vec<JsonPoolInput> = serde_json::from_str(&config_content)?;
    let weth = Address::from_str(WETH_ADDR)?;

    let mut pools = Vec::new();
    for cfg in json_configs {
        let token_a = Address::from_str(&cfg.token_a)?;
        let token_b = Address::from_str(&cfg.token_b)?;
        let token_other = if token_a == weth { token_b } else { token_a };
        pools.push(PoolConfig {
            name: cfg.name,
            router: Address::from_str(&cfg.router)?,
            quoter: Address::from_str(&cfg.quoter)?,
            fee: cfg.fee,
            token_other,
        });
    }
    info!("✅ Loaded {} V3 Pools.", pools.len());

    // 3. Block Subscription
    let mut stream = client.subscribe_blocks().await?;
    info!("Waiting for blocks...");

    loop {
        // 使用 _block 消除未使用变量警告
        let _block = match tokio::time::timeout(Duration::from_secs(15), stream.next()).await {
            Ok(Some(b)) => b,
            _ => {
                warn!("Timeout/No Block");
                continue;
            }
        };

        if gas_manager.get_loss() >= MAX_DAILY_GAS_LOSS_WEI {
            error!("💀 Daily Gas Limit Reached. Stopping.");
            break;
        }

        // --- Concurrent Logic Start ---

        // 1. 生成候选列表 (只做两两配对，O(N^2))
        let mut candidates = Vec::new();
        for i in 0..pools.len() {
            for j in 0..pools.len() {
                if i == j {
                    continue;
                }
                let (pa, pb) = (&pools[i], &pools[j]);
                if pa.token_other != pb.token_other {
                    continue;
                }
                candidates.push((pa.clone(), pb.clone()));
            }
        }

        let borrow_amount = parse_ether("0.1").unwrap();
        let client_ref = &client;
        let weth_addr_parsed: Address = WETH_ADDR.parse().unwrap();

        // 2. 高并发查询
        let results = stream::iter(candidates)
            .map(|(pa, pb)| async move {
                // Step A: WETH -> Token (Pool A)
                let quoter_a = IQuoterV2::new(pa.quoter, client_ref.clone());
                let params_a = QuoteParams {
                    token_in: weth_addr_parsed,
                    token_out: pa.token_other,
                    amount_in: borrow_amount,
                    fee: pa.fee,
                    sqrt_price_limit_x96: U256::zero(),
                };

                let out_token = match quoter_a.quote_exact_input_single(params_a).call().await {
                    Ok((amt, _, _, _)) => amt,
                    Err(_) => return None,
                };

                // Step B: Token -> WETH (Pool B)
                let quoter_b = IQuoterV2::new(pb.quoter, client_ref.clone());
                let params_b = QuoteParams {
                    token_in: pa.token_other,
                    token_out: weth_addr_parsed,
                    amount_in: out_token,
                    fee: pb.fee,
                    sqrt_price_limit_x96: U256::zero(),
                };

                let out_eth = match quoter_b.quote_exact_input_single(params_b).call().await {
                    Ok((amt, _, _, _)) => amt,
                    Err(_) => return None,
                };

                if out_eth > borrow_amount {
                    Some((pa, pb, out_eth - borrow_amount))
                } else {
                    None
                }
            })
            .buffer_unordered(30)
            .collect::<Vec<_>>()
            .await;

        // 3. 处理结果 & 发送交易
        // 🌟 修正：使用 .into_iter().flatten() 消除 Clippy 警告
        for (pa, pb, profit_wei) in results.into_iter().flatten() {
            // 简单的动态 Gas 策略: 成本预估 0.00015 ETH
            let min_cost = parse_ether("0.00015").unwrap();

            if profit_wei > min_cost {
                info!(
                    "💰 PROFIT FOUND: [{} -> {}] Profit: {} ETH",
                    pa.name,
                    pb.name,
                    format_ether(profit_wei)
                );

                // 构造交易步骤
                let steps = vec![
                    SwapStep {
                        router: pa.router,
                        token_in: weth_addr_parsed,
                        token_out: pa.token_other,
                        fee: pa.fee,
                    },
                    SwapStep {
                        router: pb.router,
                        token_in: pa.token_other,
                        token_out: weth_addr_parsed,
                        fee: pb.fee,
                    },
                ];

                let min_profit = U256::zero(); // 依赖合约回滚
                let tx_call = executor_contract.execute_arb(borrow_amount, steps, min_profit);

                let nonce = nonce_manager.get_next();
                let (base, prio) = estimate_fees(&provider)
                    .await
                    .unwrap_or((U256::from(1e8 as u64), U256::from(1e8 as u64)));

                let tx = Eip1559TransactionRequest::new()
                    .to(contract_addr)
                    .data(tx_call.calldata().unwrap())
                    .gas(500_000)
                    .max_fee_per_gas(base + prio)
                    .max_priority_fee_per_gas(prio)
                    .nonce(nonce);

                match client.send_transaction(tx, None).await {
                    Ok(p) => info!("Tx Sent: {:?}", p.tx_hash()),
                    Err(e) => {
                        error!("Tx Fail: {:?}", e);
                        let _ = nonce_manager.sync_from_chain().await;
                    }
                }
            }
        }
    }
    Ok(())
}

fn load_encrypted_config() -> Result<AppConfig> {
    let password = env::var("CONFIG_PASS").unwrap_or_else(|_| "password".to_string());
    let mut file = File::open("mev_bot.secure")?;
    let cocoon = Cocoon::new(password.as_bytes());
    let decrypted_bytes = cocoon.parse(&mut file).map_err(|e| anyhow!("{:?}", e))?;
    Ok(serde_json::from_slice(&decrypted_bytes)?)
}

async fn estimate_fees(provider: &Provider<Ipc>) -> Result<(U256, U256)> {
    let block = provider
        .get_block(BlockNumber::Latest)
        .await?
        .ok_or(anyhow!("No block"))?;
    let base = block.base_fee_per_gas.unwrap_or(U256::from(100_000_000));
    let priority = parse_units("0.1", "gwei")?.into();
    Ok((base * 120 / 100, priority))
}
