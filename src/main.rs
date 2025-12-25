use anyhow::{anyhow, Context, Result};
use chrono::Local;
use cocoon::Cocoon;
use ethers::{
    prelude::*,
    types::{Address, U256},
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
    // 保留字段以兼容配置文件
    smtp_username: String,
    smtp_password: String,
    my_email: String,
}

#[derive(Debug, Deserialize, Clone)]
struct JsonPoolInput {
    name: String,
    // token_a/b 仅用于配置文件读取，转换到 PoolConfig 后不再存储
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

// --- ABI Definitions ---
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
    // 观察模式不需要发送交易，保留此函数以防后续切回交易模式
    #[allow(dead_code)]
    fn get_next(&self) -> U256 {
        U256::from(self.nonce.fetch_add(1, Ordering::SeqCst))
    }
}

// --- Main Entry ---

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    info!("🚀 System Starting: Base V3 Observation Bot (Dry Run)");
    info!("👀 模式: 只观察不交易，打印所有微利机会");

    // 1. Config
    let config = load_encrypted_config()?;
    let provider = Arc::new(Provider::<Ipc>::connect_ipc(&config.ipc_path).await?);
    let wallet = LocalWallet::from_str(&config.private_key)?.with_chain_id(8453u64);
    let my_addr = wallet.address();
    let client = Arc::new(SignerMiddleware::new(provider.clone(), wallet.clone()));

    // 即使不发交易，加载这些结构也没坏处
    let _contract_addr: Address = config.contract_address.parse()?;
    let gas_manager = Arc::new(SharedGasManager::new("gas_state.json".to_string()));
    let _nonce_manager = Arc::new(NonceManager::new(provider.clone(), my_addr).await?);

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

        // --- Concurrent Observation Logic ---

        // 1. 生成候选列表 (两两配对)
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

        let borrow_amount = parse_ether("0.1").unwrap(); // 模拟 0.1 ETH
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

                Some((pa, pb, out_eth))
            })
            .buffer_unordered(30) // 并发度 30
            .collect::<Vec<_>>()
            .await;

        // 3. 处理结果 (只打印，不发交易)
        for (pa, pb, out_eth) in results.into_iter().flatten() {
            // 只要稍微有点价差 (out_eth > borrow_amount) 就打印
            if out_eth > borrow_amount {
                let profit_wei = out_eth - borrow_amount;

                // 估算一个大概的 Gas 成本 (0.00015 ETH)
                let estimated_cost = parse_ether("0.00015").unwrap();

                info!("👀 [观察] 发现价差: {} -> {}", pa.name, pb.name);
                info!("   投入: 0.1 ETH");
                info!("   产出: {} ETH", format_ether(out_eth));
                info!("   毛利: {} ETH", format_ether(profit_wei));

                if profit_wei > estimated_cost {
                    info!("   🔥 状态: 【盈利】 (如果开启交易，这单就赚了!)");
                } else {
                    info!(
                        "   ❄️ 状态: 【微利】 (利润 {} < 成本 {}, 不够付Gas)",
                        format_ether(profit_wei),
                        format_ether(estimated_cost)
                    );
                }
                info!("--------------------------------------------------");
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

// 辅助函数：估算 Gas 价格 (仅供日志展示使用)
#[allow(dead_code)]
async fn estimate_fees(provider: &Provider<Ipc>) -> Result<(U256, U256)> {
    let block = provider
        .get_block(BlockNumber::Latest)
        .await?
        .ok_or(anyhow!("No block"))?;
    let base = block.base_fee_per_gas.unwrap_or(U256::from(100_000_000));
    let priority = parse_units("0.1", "gwei")?.into();
    Ok((base * 120 / 100, priority))
}
