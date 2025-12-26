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
    protocol: Option<String>, // "v2" or "v3" (default v3)
}

#[derive(Clone, Debug)]
struct PoolConfig {
    name: String,
    router: Address,
    quoter: Address,
    fee: u32,
    token_other: Address,
    protocol: u8, // 0 = V3, 1 = V2
}

// --- ABI Definitions ---
abigen!(
    FlashLoanExecutor,
    r#"[
        struct SwapStep { address router; address tokenIn; address tokenOut; uint24 fee; uint8 protocol; }
        function executeArb(uint256 borrowAmount, SwapStep[] steps, uint256 minProfit) external
    ]"#;

    IQuoterV2,
    r#"[
        struct QuoteParams { address tokenIn; address tokenOut; uint256 amountIn; uint24 fee; uint160 sqrtPriceLimitX96; }
        function quoteExactInputSingle(QuoteParams params) external returns (uint256 amountOut, uint160 sqrtPriceX96After, uint32 initializedTicksCrossed, uint256 gasEstimate)
    ]"#;

    // Aerodrome Classic / Uniswap V2 Router
    IRouterV2,
    r#"[
        function getAmountsOut(uint amountIn, address[] memory path) public view returns (uint[] memory amounts)
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

// 核心：通用询价函数 (支持 V2 和 V3)
async fn get_amount_out(
    client: Arc<SignerMiddleware<Arc<Provider<Ipc>>, LocalWallet>>,
    pool: &PoolConfig,
    token_in: Address,
    token_out: Address,
    amount_in: U256,
) -> Result<U256> {
    if pool.protocol == 1 {
        // --- V2 逻辑 (Aerodrome) ---
        // 使用 Router 的 getAmountsOut，不需要 Quoter
        let router = IRouterV2::new(pool.router, client);
        let path = vec![token_in, token_out];

        // getAmountsOut 返回数组 [amountIn, amountOut]
        let amounts = router.get_amounts_out(amount_in, path).call().await?;
        if amounts.len() < 2 {
            return Err(anyhow!("Invalid V2 response"));
        }
        Ok(amounts[1])
    } else {
        // --- V3 逻辑 (Uniswap) ---
        let quoter = IQuoterV2::new(pool.quoter, client);
        let params = QuoteParams {
            token_in,
            token_out,
            amount_in,
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
    info!("🚀 System Starting: Base Universal Bot (V2 + V3)");
    info!("⚔️  支持 Uniswap V3 和 Aerodrome V2 (Classic)");

    // 1. Config
    let config = load_encrypted_config()?;
    let provider = Arc::new(Provider::<Ipc>::connect_ipc(&config.ipc_path).await?);
    let wallet = LocalWallet::from_str(&config.private_key)?.with_chain_id(8453u64);
    let my_addr = wallet.address();
    let client = Arc::new(SignerMiddleware::new(provider.clone(), wallet.clone()));

    let contract_addr: Address = config.contract_address.parse()?;
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

        // 解析协议类型: "v2" -> 1, 其他(默认v3) -> 0
        let proto_code = if let Some(p) = cfg.protocol {
            if p.to_lowercase() == "v2" {
                1
            } else {
                0
            }
        } else {
            0
        };

        pools.push(PoolConfig {
            name: cfg.name,
            router: Address::from_str(&cfg.router)?,
            quoter: Address::from_str(&cfg.quoter)?,
            fee: cfg.fee,
            token_other,
            protocol: proto_code,
        });
    }
    info!("✅ Loaded {} Pools.", pools.len());

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
                // Step A: WETH -> Token
                let out_token = match get_amount_out(
                    client_ref.clone(),
                    &pa,
                    weth_addr_parsed,
                    pa.token_other,
                    borrow_amount,
                )
                .await
                {
                    Ok(amt) => amt,
                    Err(e) => {
                        // 只有严重的错误才打印，避免刷屏
                        if !e.to_string().contains("0x") {
                            warn!("⚠️ Step A Error [{}]: {:?}", pa.name, e);
                        }
                        return None;
                    }
                };

                // Step B: Token -> WETH
                let out_eth = match get_amount_out(
                    client_ref.clone(),
                    &pb,
                    pa.token_other,
                    weth_addr_parsed,
                    out_token,
                )
                .await
                {
                    Ok(amt) => amt,
                    Err(e) => {
                        return None;
                    }
                };

                Some((pa, pb, out_eth))
            })
            .buffer_unordered(30)
            .collect::<Vec<_>>()
            .await;

        // 4. 处理结果
        info!("--- Block {} Scan ---", current_bn);
        for (pa, pb, out_eth) in results.into_iter().flatten() {
            if out_eth > borrow_amount {
                // 赚钱
                let profit = out_eth - borrow_amount;
                let profit_eth = format_ether(profit);
                let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
                let net_status = if profit > parse_ether("0.00015").unwrap() {
                    "🔥[HIGH]"
                } else {
                    "❄️[LOW]"
                };

                let log_msg = format!(
                    "[{}] {} -> {} | Profit: {} ETH ({})",
                    timestamp, pa.name, pb.name, profit_eth, net_status
                );
                info!("{}", log_msg);

                if let Ok(mut file) = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("opportunities.txt")
                {
                    let _ = writeln!(file, "{}", log_msg);
                }
            } else {
                // 亏钱 (可选: 注释掉这行以减少刷屏)
                let loss = borrow_amount - out_eth;
                info!(
                    "🧊 LOSS: {} -> {} | -{} ETH",
                    pa.name,
                    pb.name,
                    format_ether(loss)
                );
            }
        }
        info!("-----------------------");
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
