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
    fs::{self, File, OpenOptions},
    io::Write,
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
    quoter_type: Option<String>, // 新增: "v1" or "v2" (默认 v2)
}

#[derive(Clone, Debug)]
struct PoolConfig {
    name: String,
    router: Address,
    quoter: Address,
    fee: u32,
    token_other: Address,
    quoter_type: String,
}

// --- ABI Definitions ---
abigen!(
    FlashLoanExecutor,
    r#"[
        struct SwapStep { address router; address tokenIn; address tokenOut; uint24 fee; }
        function executeArb(uint256 borrowAmount, SwapStep[] steps, uint256 minProfit) external
    ]"#;

    // Uniswap V3 Quoter V2 (使用结构体)
    IQuoterV2,
    r#"[
        struct QuoteParams { address tokenIn; address tokenOut; uint256 amountIn; uint24 fee; uint160 sqrtPriceLimitX96; }
        function quoteExactInputSingle(QuoteParams params) external returns (uint256 amountOut, uint160 sqrtPriceX96After, uint32 initializedTicksCrossed, uint256 gasEstimate)
    ]"#;

    // Aerodrome / Uniswap V3 Quoter V1 (使用扁平参数)
    IQuoterV1,
    r#"[
        function quoteExactInputSingle(address tokenIn, address tokenOut, uint24 fee, uint256 amountIn, uint160 sqrtPriceLimitX96) external returns (uint256 amountOut)
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
}

// 辅助函数：执行询价，自动区分 V1/V2
async fn quote_pool(
    client: Arc<SignerMiddleware<Arc<Provider<Ipc>>, LocalWallet>>,
    pool: &PoolConfig,
    amount_in: U256,
    weth: Address,
) -> Result<U256> {
    let token_in = if pool.token_other == weth {
        pool.token_other
    } else {
        weth
    }; // 逻辑反了？
       // 修正: 我们想知道 WETH -> Token 能换多少，或者 Token -> WETH 能换多少
       // 这里传入的 pool.token_other 是非 WETH 的那个币
       // 如果我们要卖 WETH 买 Token: In=WETH, Out=Token
       // 如果我们要卖 Token 买 WETH: In=Token, Out=WETH

    // 简单起见，我们在 main loop 里显式指定 token_in/out，这里只负责发请求
    // 所以这个函数签名改一下，直接传 in/out
    Err(anyhow!("Use quote_specific instead"))
}

async fn quote_specific(
    client: Arc<SignerMiddleware<Arc<Provider<Ipc>>, LocalWallet>>,
    pool: &PoolConfig,
    token_in: Address,
    token_out: Address,
    amount: U256,
) -> Result<U256> {
    if pool.quoter_type == "v1" {
        let quoter = IQuoterV1::new(pool.quoter, client);
        let amount_out = quoter
            .quote_exact_input_single(token_in, token_out, pool.fee, amount, U256::zero())
            .call()
            .await?;
        Ok(amount_out)
    } else {
        let quoter = IQuoterV2::new(pool.quoter, client);
        let params = QuoteParams {
            token_in,
            token_out,
            amount_in: amount,
            fee: pool.fee,
            sqrt_price_limit_x96: U256::zero(),
        };
        let (amount_out, _, _, _) = quoter.quote_exact_input_single(params).call().await?;
        Ok(amount_out)
    }
}

// --- Main Entry ---

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    info!("🚀 System Starting: Base V3 HYBRID Bot (V1 & V2 Support)");
    info!("💾 模式: 记录所有盈利机会至 opportunities.txt");

    // 1. Config
    let config = load_encrypted_config()?;
    let provider = Arc::new(Provider::<Ipc>::connect_ipc(&config.ipc_path).await?);
    let wallet = LocalWallet::from_str(&config.private_key)?.with_chain_id(8453u64);
    let my_addr = wallet.address();
    let client = Arc::new(SignerMiddleware::new(provider.clone(), wallet.clone()));

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
        // 默认为 v2，如果配置写了 v1 则用 v1
        let q_type = cfg.quoter_type.unwrap_or_else(|| "v2".to_string());

        pools.push(PoolConfig {
            name: cfg.name,
            router: Address::from_str(&cfg.router)?,
            quoter: Address::from_str(&cfg.quoter)?,
            fee: cfg.fee,
            token_other,
            quoter_type: q_type,
        });
    }
    info!("✅ Loaded {} V3 Pools.", pools.len());

    // 3. Block Subscription
    let mut stream = client.subscribe_blocks().await?;
    info!("Waiting for blocks...");

    loop {
        let block = match tokio::time::timeout(Duration::from_secs(15), stream.next()).await {
            Ok(Some(b)) => b,
            _ => {
                warn!("Timeout/No Block");
                continue;
            }
        };
        let current_bn = block.number.unwrap();

        if gas_manager.get_loss() >= MAX_DAILY_GAS_LOSS_WEI {
            error!("💀 Daily Gas Limit Reached. Stopping.");
            break;
        }

        // --- Concurrent Logic ---

        let mut candidates = Vec::new();
        for i in 0..pools.len() {
            for j in 0..pools.len() {
                if i == j {
                    continue;
                }
                let (pa, pb) = (&pools[i], &pools[j]);
                // 必须是同一种币
                if pa.token_other != pb.token_other {
                    continue;
                }
                candidates.push((pa.clone(), pb.clone()));
            }
        }

        let borrow_amount = parse_ether("0.1").unwrap();
        let client_ref = &client;
        let weth_addr_parsed: Address = WETH_ADDR.parse().unwrap();

        let results = stream::iter(candidates)
            .map(|(pa, pb)| async move {
                // Step A: WETH -> Token (Pool A)
                // 卖出 WETH, 买入 Token
                let out_token = match quote_specific(
                    client_ref.clone(),
                    &pa,
                    weth_addr_parsed,
                    pa.token_other,
                    borrow_amount,
                )
                .await
                {
                    Ok(amt) => amt,
                    Err(_) => return None, // 失败静默跳过 (或者可以加日志调试)
                };

                // Step B: Token -> WETH (Pool B)
                // 卖出 Token, 买入 WETH
                let out_eth = match quote_specific(
                    client_ref.clone(),
                    &pb,
                    pa.token_other,
                    weth_addr_parsed,
                    out_token,
                )
                .await
                {
                    Ok(amt) => amt,
                    Err(_) => return None,
                };

                Some((pa, pb, out_eth))
            })
            .buffer_unordered(30)
            .collect::<Vec<_>>()
            .await;

        // 4. 处理结果
        let mut profit_count = 0;
        for (pa, pb, out_eth) in results.into_iter().flatten() {
            // 只看赚钱的
            if out_eth > borrow_amount {
                profit_count += 1;
                let profit = out_eth - borrow_amount;
                let profit_eth = format_ether(profit);
                let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();

                let gas_cost = parse_ether("0.00015").unwrap();
                let net_status = if profit > gas_cost {
                    "🔥[高利]"
                } else {
                    "❄️[微利]"
                };

                let log_msg = format!(
                    "[{}] Block: {} | {} | {} -> {} | Profit: {} ETH",
                    timestamp, current_bn, net_status, pa.name, pb.name, profit_eth
                );

                info!("{}", log_msg);

                // 写入文件
                if let Ok(mut file) = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("opportunities.txt")
                {
                    let _ = writeln!(file, "{}", log_msg);
                }
            }
        }

        if profit_count > 0 {
            info!(
                "--- Block {} Found {} opportunities ---",
                current_bn, profit_count
            );
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
