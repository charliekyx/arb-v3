use anyhow::{anyhow, Context, Result};
use chrono::Local;
use cocoon::Cocoon;
use ethers::{
    prelude::*,
    types::{Address, U256},
    utils::{format_ether, parse_ether},
};
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    env,
    fs::{self, File, OpenOptions},
    io::Write,
    str::FromStr,
    sync::{atomic::AtomicU64, Arc, Mutex},
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
    quoter: String, // 注意：CL 模式下这里填 Pool 地址
    fee: u32,
    protocol: Option<String>,
}

#[derive(Clone, Debug)]
struct PoolConfig {
    name: String,
    router: Address,
    quoter: Address, // CL 模式下这里是 Pool 地址
    fee: u32,
    token_other: Address,
    protocol: u8, // 0=V3, 1=V2, 2=CL
}

// --- ABI Definitions ---
abigen!(
    FlashLoanExecutor,
    r#"[
        struct SwapStep { address router; address tokenIn; address tokenOut; uint24 fee; uint8 protocol; }
        function executeArb(uint256 borrowAmount, SwapStep[] steps, uint256 minProfit) external
    ]"#;

    // ✅ Uniswap V3 Quoter
    IQuoterV2,
    r#"[
        struct QuoteParams { address tokenIn; address tokenOut; uint256 amountIn; uint24 fee; uint160 sqrtPriceLimitX96; }
        function quoteExactInputSingle(QuoteParams params) external returns (uint256 amountOut, uint160 sqrtPriceX96After, uint32 initializedTicksCrossed, uint256 gasEstimate)
    ]"#;

    // ✅ CL Pool Slot0 (直接读池子状态)
    ICLPool,
    r#"[
        function slot0() external view returns (uint160 sqrtPriceX96, int24 tick, uint16 observationIndex, uint16 observationCardinality, uint16 observationCardinalityNext, bool unlocked)
        function token0() external view returns (address)
    ]"#;

    IAerodromePair,
    r#"[
        function reserve0() external view returns (uint256)
        function reserve1() external view returns (uint256)
        function token0() external view returns (address)
    ]"#
);

const WETH_ADDR: &str = "0x4200000000000000000000000000000000000006";
const MAX_DAILY_GAS_LOSS_WEI: u128 = 20_000_000_000_000_000;

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

// 🔥 本地计算 sqrtPriceX96 -> Output
// amountOut = amountIn * (sqrtPriceX96 / 2^96)^2
// 注意：这只是瞬时价格，没算滑点，但用来做“观察者”足够且极快
fn calculate_v3_amount_out(
    amount_in: U256,
    sqrt_price_x96: U256,
    token_in: Address,
    token0: Address,
) -> U256 {
    let q96 = U256::from(2).pow(U256::from(96));

    // Price = (sqrtPrice / Q96)^2
    // 如果 token_in == token0: price = token1 / token0. amountOut = amountIn * price
    // 如果 token_in == token1: price = token0 / token1. amountOut = amountIn * (1/price)

    // 为了防止溢出，使用 U256 的 mul_div
    // amountOut = amountIn * sqrtPrice * sqrtPrice / Q96 / Q96

    if token_in == token0 {
        // Token0 -> Token1
        // output = input * (sqrtPrice / 2^96)^2
        // output = input * sqrtPrice * sqrtPrice / 2^192
        let numerator = amount_in
            .saturating_mul(sqrt_price_x96)
            .saturating_mul(sqrt_price_x96);
        let denominator = q96.saturating_mul(q96);
        numerator.checked_div(denominator).unwrap_or_default()
    } else {
        // Token1 -> Token0
        // output = input / (sqrtPrice / 2^96)^2
        // output = input * 2^192 / (sqrtPrice * sqrtPrice)
        let numerator = amount_in.saturating_mul(q96).saturating_mul(q96);
        let denominator = sqrt_price_x96.saturating_mul(sqrt_price_x96);
        numerator.checked_div(denominator).unwrap_or_default()
    }
}

// 核心：通用询价函数
async fn get_amount_out(
    client: Arc<SignerMiddleware<Arc<Provider<Ipc>>, LocalWallet>>,
    pool: &PoolConfig,
    token_in: Address,
    token_out: Address,
    amount_in: U256,
) -> Result<U256> {
    if pool.protocol == 1 {
        // --- V2 Logic (Aerodrome Basic) ---
        let pair = IAerodromePair::new(pool.quoter, client.clone());
        let r0 = pair
            .reserve_0()
            .call()
            .await
            .map_err(|e| anyhow!("V2 r0: {}", e))?;
        let r1 = pair
            .reserve_1()
            .call()
            .await
            .map_err(|e| anyhow!("V2 r1: {}", e))?;
        let t0 = pair
            .token_0()
            .call()
            .await
            .map_err(|e| anyhow!("V2 t0: {}", e))?;

        let (reserve_in, reserve_out) = if t0 == token_in { (r0, r1) } else { (r1, r0) };
        if reserve_in.is_zero() || reserve_out.is_zero() {
            return Err(anyhow!("Empty V2 reserves"));
        }

        let fee_bps = U256::from(pool.fee);
        let amount_in_with_fee = amount_in * (U256::from(1000000) - fee_bps);
        let numerator = amount_in_with_fee * reserve_out;
        let denominator = (reserve_in * U256::from(1000000)) + amount_in_with_fee;
        Ok(numerator / denominator)
    } else if pool.protocol == 2 {
        // --- CL Logic (Aerodrome Slipstream) -> Local Calc via Slot0 ---
        // 既然 Quoter 这么坑，我们直接读池子状态
        // 注意：pools.json 里 CL 的 'quoter' 字段必须填【池子地址】
        let pool_contract = ICLPool::new(pool.quoter, client.clone());
        let (sqrt_price, _, _, _, _, _) = pool_contract
            .slot_0()
            .call()
            .await
            .map_err(|e| anyhow!("CL slot0: {}", e))?;
        let token0 = pool_contract
            .token_0()
            .call()
            .await
            .map_err(|e| anyhow!("CL token0: {}", e))?;

        // 计算无滑点价格
        let raw_out = calculate_v3_amount_out(amount_in, U256::from(sqrt_price), token_in, token0);

        // 手动扣除手续费 (Aerodrome CL 费率通常是 3000 -> 0.3%)
        // CL 的 fee 是在 swap 过程中扣除的，这里简单模拟 output * (1 - fee)
        let fee_ppm = U256::from(pool.fee); // 假设是 3000
        let out_after_fee = raw_out * (U256::from(1000000) - fee_ppm) / U256::from(1000000);

        Ok(out_after_fee)
    } else {
        // --- V3 Logic (Uniswap V3) -> 使用 Quoter V2 ---
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

// --- Main ---

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    info!("🚀 System Starting: Base Bot V3.9 (Local Calc Mode)");
    info!("🔥 模式: V2(Reserves) + CL(Slot0 Local) + V3(QuoterV2)");

    let config = load_encrypted_config()?;
    let provider = Arc::new(Provider::<Ipc>::connect_ipc(&config.ipc_path).await?);
    let wallet = LocalWallet::from_str(&config.private_key)?.with_chain_id(8453u64);
    let client = Arc::new(SignerMiddleware::new(provider.clone(), wallet.clone()));
    let gas_manager = Arc::new(SharedGasManager::new("gas_state.json".to_string()));

    let config_content = fs::read_to_string("pools.json").context("Failed to read pools.json")?;
    let json_configs: Vec<JsonPoolInput> = serde_json::from_str(&config_content)?;
    let weth = Address::from_str(WETH_ADDR)?;

    let mut pools = Vec::new();
    for cfg in json_configs {
        let token_a = Address::from_str(&cfg.token_a)?;
        let token_b = Address::from_str(&cfg.token_b)?;
        let token_other = if token_a == weth { token_b } else { token_a };

        let proto_str = cfg.protocol.unwrap_or("v3".to_string()).to_lowercase();
        let proto_code = if proto_str == "v2" {
            1
        } else if proto_str == "cl" {
            2
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

    let mut stream = client.subscribe_blocks().await?;
    info!("Waiting for blocks...");

    loop {
        let block = match tokio::time::timeout(Duration::from_secs(15), stream.next()).await {
            Ok(Some(b)) => b,
            _ => {
                warn!("Timeout");
                continue;
            }
        };
        let current_bn = block.number.unwrap();

        if gas_manager.get_loss() >= MAX_DAILY_GAS_LOSS_WEI {
            error!("💀 Daily Gas Limit Reached.");
            break;
        }

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

        let borrow_amount = parse_ether("0.001").unwrap();
        let client_ref = &client;
        let weth_addr_parsed: Address = WETH_ADDR.parse().unwrap();

        let results = stream::iter(candidates)
            .map(|(pa, pb)| async move {
                // Step A
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
                        warn!("⚠️ Step A [{}] Fail: {:?}", pa.name, e);
                        return None;
                    }
                };

                // Step B
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
                        warn!("⚠️ Step B [{}] Fail: {:?}", pb.name, e);
                        return None;
                    }
                };
                Some((pa, pb, out_eth))
            })
            .buffer_unordered(30)
            .collect::<Vec<_>>()
            .await;

        info!("--- Block {} Check ---", current_bn);
        for (pa, pb, out_eth) in results.into_iter().flatten() {
            if out_eth > borrow_amount {
                let profit = out_eth - borrow_amount;
                let profit_eth = format_ether(profit);
                let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
                let net_status = if profit > parse_ether("0.00015").unwrap() {
                    "🔥[HIGH]"
                } else {
                    "❄️[LOW]"
                };
                let log_msg = format!(
                    "[{}] 💰 PROFIT: {} -> {} | +{} ETH ({})",
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
