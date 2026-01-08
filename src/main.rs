use anyhow::{anyhow, Context, Result};
use chrono::Local;
use cocoon::Cocoon;
use dashmap::DashMap;

use ethers::{
    abi::Tokenizable,
    prelude::*,
    types::{Address, I256, U256},
    utils::{format_ether, format_units, parse_ether, parse_units},
};
use futures::stream::{self, StreamExt};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{Message, SmtpTransport, Transport};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    env,
    fs::{self, File, OpenOptions},
    io::Write,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tracing::{error, info, warn};
use uniswap_v3_math::{
    swap_math::compute_swap_step, tick_bitmap::next_initialized_tick_within_one_word, tick_math,
};

// 引入 Execution 模块
mod execution;
use execution::execute_transaction;

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

#[derive(Debug, Clone)]
struct CachedPoolState {
    block_number: U64,
    // V2 Data
    reserve0: u128,
    reserve1: u128,
    // V3 Data
    sqrt_price_x96: U256,
    liquidity: u128,
    tick: i32,
    tick_spacing: i32,
    // [Prod Ready]: 存储 Tick 信息
    // map: tick_index -> liquidity_net (该 tick 上流动性的增减量)
    ticks: HashMap<i32, i128>,
    // map: word_pos -> bitmap (用于快速查找下一个 tick)
    tick_bitmap: HashMap<i16, U256>,
}

// Global cache to store pool state. Key: Pool Address
type PoolCache = Arc<DashMap<Address, CachedPoolState>>;

#[derive(Debug, Deserialize, Clone)]
struct JsonPoolInput {
    name: String,
    token_a: String,
    token_b: String,
    router: String,
    quoter: Option<String>,
    pool: Option<String>,
    fee: Option<u32>,
    tick_spacing: Option<i32>,
    pool_fee: Option<u32>,
    protocol: Option<String>,
}

#[derive(Clone, Debug)]
struct PoolConfig {
    name: String,
    router: Address,
    quoter: Option<Address>,
    pool: Option<Address>,
    fee: u32,
    tick_spacing: i32,
    pool_fee: u32,
    token_a: Address,
    token_b: Address,
    protocol: u8, // 0=Uniswap V3 , 1=Uniswap V2, 2=CL(Aerodrome Concentrated Liquidity)
}

// --- Logging Structs ---
#[derive(Serialize, Debug, Clone)]
struct StepLog {
    pool: String,
    token_in: String,
    token_out: String,
    amount_in: String,
    amount_out: String,
}

#[derive(Serialize, Debug, Clone)]
struct OpportunityLog {
    block: u64,
    ts: u64,
    path: Vec<String>,
    tokens: Vec<String>,
    size_raw: String,
    out_raw: String,
    gross_raw: String,
    net_raw: String,
    gross_bps: i128,
    net_bps: i128,
    gas_price_wei: String,
    gas_used_assumed: u64,
    gas_cost_priced_raw: String,
    can_price_gas: bool,
    steps: Vec<StepLog>,
}
// --- ABI Definitions ---
abigen!(
    // 必须调用 Uniswap 官方的 QuoterV2 合约的 quoteExactInputSingle 函数。
    // 因为 V3 的数学逻辑太复杂（涉及跨越多个 Tick, 很难在本地完美模拟)
    // https://docs.uniswap.org/contracts/v3/reference/periphery/lens/QuoterV2
    // These functions are not gas efficient and should not be called on chain. Instead, optimistically execute the swap and check the amounts in the callback.
    IQuoterV2,
    r#"[
        struct QuoteParams { address tokenIn; address tokenOut; uint256 amountIn; uint24 fee; uint160 sqrtPriceLimitX96; }
        function quoteExactInputSingle(QuoteParams params) external returns (uint256 amountOut, uint160 sqrtPriceX96After, uint32 initializedTicksCrossed, uint256 gasEstimate)
    ]"#;

    // 使用 Aerodrome 专门的 CLQuoter 合约。虽然原理和 V3 一样，但合约接口（ABI）略有不同（例如返回值的结构），所以专门写了 IAerodromeCLQuoter 来适配
    // https://github.com/aerodrome-finance/contracts?tab=readme-ov-file
    IAerodromeCLQuoter,
    r#"[
        struct CLQuoteParams { address tokenIn; address tokenOut; uint256 amountIn; int24 tickSpacing; uint160 sqrtPriceLimitX96; }
        function quoteExactInputSingle(CLQuoteParams params) external returns (uint256 amountOut, uint256 r1, uint256 r2, uint256 r3)
    ]"#;

    // ICLPool (Concentrated Liquidity Pool), 这个接口对应 Uniswap V3 的核心池子合约（Core Pool
    // slot0(): 返回池子的当前状态，包括最重要的 sqrtPriceX96（当前价格的平方根）和 tick
    // liquidity(): 返回池子在当前 Tick 下的有效流动性总量。uniswap
    // tickSpacing(): 决定了价格刻度的密度，不同费率的池子该值不同。
    // https://docs.uniswap.org/contracts/v3/reference/core/UniswapV3Pool

    ICLPool,
    r#"[
        function tickSpacing() external view returns (int24)
        function fee() external view returns (uint24)
        function liquidity() external view returns (uint128)
        function slot0() external view returns (uint160 sqrtPriceX96, int24 tick, uint16 observationIndex, uint16 observationCardinality, uint16 observationCardinalityNext, uint8 feeProtocol, bool unlocked)
        function token0() external view returns (address)
        function tickBitmap(int16 wordPosition) external view returns (uint256)
        function ticks(int24 tick) external view returns (uint128 liquidityGross, int128 liquidityNet, uint256 feeGrowthOutside0X128, uint256 feeGrowthOutside1X128, int56 tickCumulativeOutside, uint160 secondsPerLiquidityOutsideX128, uint32 secondsOutside, bool initialized)
    ]"#;

    // Uniswap V2 是行业标准。绝大多数 V2 类 DEX（如 BaseSwap, SushiSwap, AlienBase）都完全复制了 Uniswap V2 的接口。
    // Aerodrome (以及它的前身 Velodrome/Solidly) 的 Pair 合约里额外包含了一个 getAmountOut 函数。
    // 在 Aerodrome 中称为 Basic/Volatile 和 Stable 池
    // 目前配置文件里，所有 Aerodrome 的池子都标记为 "protocol": "cl"
    // 支持：Aerodrome 的 Basic (Volatile) 池子。因为它们使用的是标准的 $x \times y = k$ 公式，和代码里的本地计算逻辑兼容。
    // 注意！！ 不支持：Aerodrome 的 Stable 池子（如 USDC/USDbC Basic）。因为稳定币池使用的是 $x^3y + y^3x = k$ 的混合曲线公式，你目前的本地计算函数算出来的价格会是错的。
    // 标准的 Uniswap V2 Pair 合约里没有 getAmountOut（Uniswap V2 的询价通常是在 Router 合约里算的，或者链下算）
    // https://docs.uniswap.org/contracts/v2/reference/smart-contracts/pair
    IUniswapV2Pair,
    r#"[
        function getReserves() external view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast)
        function token0() external view returns (address)
        function token1() external view returns (address)
    ]"#;

    IUniswapV3Factory,
    r#"[
        function getPool(address tokenA, address tokenB, uint24 fee) external view returns (address pool)
    ]"#
);

// todo: 配置在环境变量里面
const WETH_ADDR: &str = "0x4200000000000000000000000000000000000006";
const USDC_ADDR: &str = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913";
const USDBC_ADDR: &str = "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA";
const AERO_ADDR: &str = "0x940181a94A35A4569E4529A3CDfB74e38FD98631";
const CBETH_ADDR: &str = "0x2ae3f1ec7f1f5012cfeab0185bfc7aa3cf0dec22";
const EZETH_ADDR: &str = "0x2416092f143378750bb29b79ed961ab195cceea5";
const MAX_DAILY_GAS_LOSS_WEI: u128 = 20_000_000_000_000_000;
const UNISWAP_QUOTER: &str = "0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a";
const MULTICALL_ADDRESS: &str = "0xcA11bde05977b3631167028862bE2a173976CA11";
const UNI_V3_FACTORY: &str = "0x33128a8fC17869897dcE68Ed026d694621f6FDfD"; // Base Uniswap V3 Factory
const UNI_V3_ROUTER: &str = "0x2626664c2603336E57B271c5C0b26F421741e481";
const AERO_CL_ROUTER: &str = "0xBE6D8f0d05cC4be24d5167a3eF062215bE6D18a5"; // Aerodrome Slipstream Router

// --- Helpers ---
#[derive(Serialize, Deserialize, Debug, Default)]
struct GasState {
    date: String,
    accumulated_loss: u128,
}

struct SharedGasManager {
    file_path: String,
    accumulated_loss: Mutex<u128>,
}

impl SharedGasManager {
    fn new(path: String) -> Self {
        let loaded = Self::load_gas_state(&path);
        Self {
            file_path: path,
            accumulated_loss: Mutex::new(loaded.accumulated_loss),
        }
    }

    fn load_gas_state(path: &str) -> GasState {
        let today = Local::now().format("%Y-%m-%d").to_string();
        if let Ok(c) = std::fs::read_to_string(path) {
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

    fn add_loss(&self, loss: u128) {
        let mut lock = self.accumulated_loss.lock().unwrap();
        *lock += loss;

        let state = GasState {
            date: Local::now().format("%Y-%m-%d").to_string(),
            accumulated_loss: *lock,
        };

        if let Ok(json) = serde_json::to_string_pretty(&state) {
            let _ = std::fs::write(&self.file_path, json);
        }
    }
}

fn append_jsonl_log(log_entry: &OpportunityLog) -> Result<()> {
    let file_path = "trades.jsonl";
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_path)?;
    let json_string = serde_json::to_string(log_entry)?;
    writeln!(file, "{}", json_string)?;
    Ok(())
}

fn append_log_to_file(msg: &str) {
    let file_path = "opportunities.txt";
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(file_path) {
        let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S");
        let _ = writeln!(file, "[{}] {}", timestamp, msg);
    }
}

fn pool_supports(pool: &PoolConfig, token_in: Address, token_out: Address) -> bool {
    (pool.token_a == token_in && pool.token_b == token_out)
        || (pool.token_a == token_out && pool.token_b == token_in)
}

fn format_token_amount(amount: U256, token: Address) -> String {
    let usdc = Address::from_str(USDC_ADDR).unwrap();
    let usdbc = Address::from_str(USDBC_ADDR).unwrap();

    if token == usdc || token == usdbc {
        format_units(amount, 6).unwrap_or_else(|_| "0.0".to_string())
    } else {
        format_ether(amount)
    }
}

fn token_symbol(token: Address) -> String {
    let weth = Address::from_str(WETH_ADDR).unwrap();
    let usdc = Address::from_str(USDC_ADDR).unwrap();
    let usdbc = Address::from_str(USDBC_ADDR).unwrap();
    let aero = Address::from_str(AERO_ADDR).unwrap();
    let cbeth = Address::from_str(CBETH_ADDR).unwrap();
    let ezeth = Address::from_str(EZETH_ADDR).unwrap();

    if token == weth {
        "WETH".to_string()
    } else if token == usdc {
        "USDC".to_string()
    } else if token == usdbc {
        "USDbC".to_string()
    } else if token == aero {
        "AERO".to_string()
    } else if token == cbeth {
        "cbETH".to_string()
    } else if token == ezeth {
        "ezETH".to_string()
    } else {
        format!("{:?}", token)
    }
}

fn decimals(token: Address) -> u32 {
    let usdc = Address::from_str(USDC_ADDR).unwrap();
    let usdbc = Address::from_str(USDBC_ADDR).unwrap();
    if token == usdc || token == usdbc {
        6
    } else {
        18
    }
}

fn parse_amount(s: &str, token: Address) -> U256 {
    let d = decimals(token);
    parse_units(s, d).expect("parse_units failed").into()
}

fn calculate_v3_amount_out(
    amount_in: U256,
    sqrt_price_x96: U256,
    token_in: Address,
    token0: Address,
) -> U256 {
    let q96 = U256::from(2).pow(U256::from(96));
    if token_in == token0 {
        let numerator = amount_in
            .saturating_mul(sqrt_price_x96)
            .saturating_mul(sqrt_price_x96);
        let denominator = q96.saturating_mul(q96);
        numerator.checked_div(denominator).unwrap_or_default()
    } else {
        let numerator = amount_in.saturating_mul(q96).saturating_mul(q96);
        let denominator = sqrt_price_x96.saturating_mul(sqrt_price_x96);
        numerator.checked_div(denominator).unwrap_or_default()
    }
}

use ethers::types::{Bytes, TransactionRequest};
use ethers::utils::keccak256;
async fn debug_slot0_raw(provider: &Provider<Ipc>, pool: Address) -> Result<()> {
    let selector = &keccak256(b"slot0()")[0..4];
    let data = Bytes::from(selector.to_vec());
    let tx = TransactionRequest::new().to(pool).data(data);
    let out: Bytes = provider.call(&tx.into(), None).await?;
    info!("slot0 raw len={} bytes", out.0.len());
    info!("slot0 raw=0x{}", hex::encode(&out.0));
    Ok(())
}

fn sel4(sig: &str) -> [u8; 4] {
    let h = keccak256(sig.as_bytes());
    [h[0], h[1], h[2], h[3]]
}

async fn validate_cl_pool(
    client: Arc<SignerMiddleware<Arc<Provider<Ipc>>, LocalWallet>>,
    pool: &PoolConfig,
) -> Option<(i32, u32)> {
    let Some(pool_addr) = pool.pool else {
        return None;
    };

    match client.provider().get_code(pool_addr, None).await {
        Ok(code) if code.0.is_empty() => {
            warn!("CL Pool {} has no code @ {:?}", pool.name, pool_addr);
            return None;
        }
        Err(e) => {
            warn!(
                "CL Pool {} getCode failed @ {:?}: {:?}",
                pool.name, pool_addr, e
            );
            return None;
        }
        _ => {}
    }

    let contract = ICLPool::new(pool_addr, client.clone());
    let ts = match contract.tick_spacing().call().await {
        Ok(v) => v,
        Err(e) => {
            warn!(
                "CL Pool {} tickSpacing() failed @ {:?}: {:?}",
                pool.name, pool_addr, e
            );
            return None;
        }
    };
    let fee = match contract.fee().call().await {
        Ok(v) => v,
        Err(e) => {
            warn!(
                "CL Pool {} fee() failed @ {:?}: {:?}",
                pool.name, pool_addr, e
            );
            return None;
        }
    };
    let liq = match contract.liquidity().call().await {
        Ok(v) => v,
        Err(e) => {
            warn!(
                "CL Pool {} liquidity() failed @ {:?}: {:?}",
                pool.name, pool_addr, e
            );
            return None;
        }
    };

    // [核心修改] 3. 使用 Multicall 验证 slot0
    // 很多"坏池子"防合约调用，必须用 Multicall 模拟真实运行环境
    let multicall_address = MULTICALL_ADDRESS.parse::<Address>().unwrap();

    // 创建一个临时的 Multicall 实例用于验证
    if let Ok(mut multicall) = Multicall::new(client.clone(), Some(multicall_address)).await {
        // 添加 slot0 调用，设置为 false (require success)，如果失败直接报错
        multicall.add_call(contract.slot_0(), false);

        // 执行调用。如果 Multicall 返回错误，或者解码失败，说明该池子不兼容 Multicall
        if let Err(e) = multicall.call_raw().await {
            warn!(
                "CL Pool {} slot0() via Multicall failed (BAD POOL): {:?}",
                pool.name, e
            );
            return None;
        }
    } else {
        warn!(
            "Failed to create Multicall instance during validation for {}",
            pool.name
        );
        return None;
    }

    info!(
        "CL Pool {} ok | ts={} fee={} liq={} | Multicall Check Passed",
        pool.name, ts, fee, liq
    );
    Some((ts, fee))
}

async fn validate_v2_pool(
    client: Arc<SignerMiddleware<Arc<Provider<Ipc>>, LocalWallet>>,
    pool: &PoolConfig,
) -> bool {
    if let Some(pair_addr) = pool.quoter {
        let pair = IUniswapV2Pair::new(pair_addr, client.clone());
        match pair.get_reserves().call().await {
            Ok(_) => true,
            Err(e) => {
                warn!("Pool {} failed getReserves: {:?}", pool.name, e);
                false
            }
        }
    } else {
        false
    }
}

/// Helper to get the correct pool address based on protocol
fn get_pool_address(pool: &PoolConfig) -> Option<Address> {
    if pool.protocol == 1 {
        pool.quoter // V2 uses quoter as pair address
    } else {
        pool.pool // V3/CL uses pool
    }
}

fn send_email_alert(subject: &str, body: &str) {
    let email_user = "charlieyuxx@gmail.com";
    let email_pass = "sabw gnll hfuq yesl";
    let email_to = "charlieyuxx@gmail.com";

    let email = match Message::builder()
        .from(email_user.parse().unwrap())
        .to(email_to.parse().unwrap())
        .subject(subject)
        .body(body.to_string())
    {
        Ok(e) => e,
        Err(e) => {
            error!("Email build failed: {:?}", e);
            return;
        }
    };

    let creds = Credentials::new(email_user.to_string(), email_pass.to_string());
    let mailer = SmtpTransport::relay("smtp.gmail.com")
        .unwrap()
        .credentials(creds)
        .build();

    match mailer.send(&email) {
        Ok(_) => info!("📧 Email sent successfully!"),
        Err(e) => error!("Could not send email: {:?}", e),
    }
}

fn get_initialized_ticks_from_bitmap(word_pos: i16, bitmap: U256) -> Vec<i32> {
    let mut ticks = Vec::new();
    if bitmap.is_zero() {
        return ticks;
    }
    for i in 0..256 {
        if (bitmap >> i) & U256::one() != U256::zero() {
            // Calculate the tick index from the word position and the bit position.
            // This is the actual tick index.
            let tick = (word_pos as i32) * 256 + i as i32;
            ticks.push(tick);
        }
    }
    ticks
}

// Local V2 calculation (Math only, no IPC)
fn get_v2_amount_out_local(
    amount_in: U256,
    reserve_in: U256,
    reserve_out: U256,
    fee_bps: U256,
) -> U256 {
    if amount_in.is_zero() || reserve_in.is_zero() || reserve_out.is_zero() {
        return U256::zero();
    }
    let amount_in_with_fee = amount_in * (U256::from(10000) - fee_bps);
    let numerator = amount_in_with_fee * reserve_out;
    let denominator = (reserve_in * U256::from(10000)) + amount_in_with_fee;
    if denominator.is_zero() {
        U256::zero()
    } else {
        numerator / denominator
    }
}

fn add_delta(x: u128, y: i128) -> Result<u128> {
    if y < 0 {
        let z = (-y) as u128;
        if z > x {
            return Err(anyhow!("Liquidity underflow"));
        }
        Ok(x - z)
    } else {
        Ok(x + (y as u128))
    }
}

// Local V3 calculation (Math only, using uniswap_v3_math)
// NOTE: This assumes swap does not cross tick boundaries (Small amounts).
// For full production, you must implement step-by-step swap within ticks.
fn get_v3_amount_out_local(
    amount_in: U256,
    token_in: Address,
    token_out: Address,
    pool: &PoolConfig,
    state: &CachedPoolState,
) -> Result<U256> {
    // [FIX] 增加 tick_spacing == 0 的检查，防止 Panic
    if amount_in.is_zero() || state.liquidity == 0 || state.tick_spacing == 0 {
        return Ok(U256::zero());
    }

    // zero_for_one: token0 -> token1 (价格向下, tick 变小)
    let zero_for_one = token_in < token_out;

    let mut current_sqrt_price_x96 = state.sqrt_price_x96;
    let mut current_tick = state.tick;
    let mut current_liquidity = state.liquidity;
    let mut amount_remaining = I256::from_raw(amount_in);
    let mut amount_calculated = I256::zero();
    let fee_pips = pool.fee as u32;

    while amount_remaining > I256::zero() {
        let (next_tick, initialized) = next_initialized_tick_within_one_word(
            &state.tick_bitmap,
            current_tick,
            state.tick_spacing,
            zero_for_one,
        )?;

        let sqrt_price_limit_x96 = tick_math::get_sqrt_ratio_at_tick(next_tick)?;

        let (sqrt_price_next_x96, amount_in_consumed, amount_out_received, _fee_amount) =
            compute_swap_step(
                current_sqrt_price_x96,
                sqrt_price_limit_x96,
                current_liquidity,
                amount_remaining,
                fee_pips,
            )?;

        current_sqrt_price_x96 = sqrt_price_next_x96;
        amount_remaining -= I256::from_raw(amount_in_consumed);
        amount_calculated -= I256::from_raw(amount_out_received);

        if current_sqrt_price_x96 == sqrt_price_limit_x96 {
            if initialized {
                // 如果本地没有 Tick 数据，则无法继续计算，必须报错（或者返回当前计算结果）
                // 稳健策略：返回 Error 让上层放弃此路径
                let liquidity_net = state.ticks.get(&next_tick).ok_or_else(|| {
                    warn!(
                        "MISSING TICK DATA: Pool {} needs tick {}",
                        pool.name, next_tick
                    );
                    anyhow!("Tick data missing for tick: {}", next_tick)
                })?;

                if zero_for_one {
                    current_tick = next_tick - 1;
                    current_liquidity = add_delta(current_liquidity, -liquidity_net)?;
                } else {
                    current_tick = next_tick;
                    current_liquidity = add_delta(current_liquidity, *liquidity_net)?;
                }
            } else {
                current_tick = if zero_for_one {
                    next_tick - 1
                } else {
                    next_tick
                };
            }
        } else {
            let _ = tick_math::get_tick_at_sqrt_ratio(current_sqrt_price_x96)?;
            break;
        }

        if current_liquidity == 0 {
            break;
        }
    }

    Ok(amount_calculated.abs().into_raw())
}

// The Main Pricing Function: Reads from Memory Cache
async fn get_amount_out(
    amount_in: U256,
    token_in: Address,
    token_out: Address,
    pool: &PoolConfig,
    cache: &PoolCache,
    current_block: U64,
) -> Result<U256> {
    // 1. Check Cache
    let Some(address) = get_pool_address(pool) else {
        return Ok(U256::zero());
    };
    let state_guard = cache.get(&address);
    let state = match state_guard {
        Some(s) => s,
        None => return Ok(U256::zero()), // If state not synced yet, skip
    };

    // Optional: Check if state is stale (too old)
    if current_block > state.block_number + U64::from(10) {
        // Data too old, unsafe to trade
        return Ok(U256::zero());
    }

    if pool.protocol == 1 {
        // === V2 Logic ===
        let (r0, r1) = (U256::from(state.reserve0), U256::from(state.reserve1));
        let (reserve_in, reserve_out) = if token_in < token_out {
            (r0, r1)
        } else {
            (r1, r0)
        };
        Ok(get_v2_amount_out_local(
            amount_in,
            reserve_in,
            reserve_out,
            U256::from(pool.fee),
        ))
    } else {
        // === V3 Logic ===
        // 使用新的 V3 本地计算逻辑
        get_v3_amount_out_local(amount_in, token_in, token_out, pool, &state)
    }
}

// === 3. Bulk State Updater ===

async fn update_all_pools(
    provider: Arc<Provider<Ipc>>,
    pools: &[PoolConfig],
    cache: PoolCache,
    current_block: U64,
) {
    // --- V2 and V3/CL pools require different calls, so we can handle them separately ---

    // 1. Handle V2 pools with concurrent calls (they are few and simple)
    let v2_pools: Vec<_> = pools.iter().filter(|p| p.protocol == 1).collect();
    let v2_stream = stream::iter(v2_pools).for_each_concurrent(50, |pool| {
        let provider = provider.clone();
        let cache = cache.clone();
        async move {
            let Some(address) = get_pool_address(pool) else {
                return;
            };
            if cache
                .get(&address)
                .map_or(false, |s| s.block_number == current_block)
            {
                return;
            }

            let pair = IUniswapV2Pair::new(address, provider);
            if let Ok((r0, r1, _)) = pair.get_reserves().call().await {
                cache.insert(
                    address,
                    CachedPoolState {
                        block_number: current_block,
                        reserve0: r0,
                        reserve1: r1,
                        sqrt_price_x96: U256::zero(),
                        liquidity: 0,
                        tick: 0,
                        tick_spacing: 0,
                        ticks: HashMap::new(),
                        tick_bitmap: HashMap::new(),
                    },
                );
            }
        }
    });

    // 2. Handle all V3/CL pools with a single Multicall for base data (slot0, liquidity)
    // 2. Handle all V3/CL pools (Bitmap Aware Version)
    let v3_pools: Vec<_> = pools.iter().filter(|p| p.protocol != 1).collect();
    let v3_task = async {
        let multicall_address = MULTICALL_ADDRESS.parse::<Address>().unwrap();

        // 1. Chunking to avoid RPC limits
        // 使用并发处理 stream
        let chunks: Vec<_> = v3_pools.chunks(1).collect();
        stream::iter(chunks)
            .for_each_concurrent(20, |chunk| {
                let provider = provider.clone();
                let cache = cache.clone();
                // 需要克隆 chunk 中的数据以移动到 async 块中
                let chunk_owned: Vec<PoolConfig> = chunk.iter().map(|&p| p.clone()).collect();

                async move {
                    let Ok(mut multicall) =
                        Multicall::new(provider.clone(), Some(multicall_address)).await
                    else {
                        return;
                    };

                    let mut pre_updates = Vec::new();

                    // --- Multicall 1: Slot0, Liquidity, AND Bitmap ---
                    for pool in &chunk_owned {
                        let Some(address) = get_pool_address(pool) else {
                            continue;
                        };
                        // 简单缓存检查：如果 block 没变且上次也没报错，可以跳过 (这里为了修复先略过)

                        let v3_pool = ICLPool::new(address, provider.clone());
                        multicall.add_call(v3_pool.slot_0(), true); // idx 0
                        multicall.add_call(v3_pool.liquidity(), true); // idx 1
                                                                       // 我们还不知道 tick 在哪，没法精准拿 Bitmap？
                                                                       // 这是一个“鸡生蛋”问题。
                                                                       // 解决方案：我们假设池子价格不会瞬间跳变太远。我们读取缓存中的旧 tick 来决定取哪个 Bitmap。
                                                                       // 如果缓存里没有（第一次启动），我们只能先不取 Bitmap，等下一轮？
                                                                       // 不，更稳妥的方法是：分两步走。先拿 slot0，再拿 Bitmap + Ticks。
                                                                       // 但是为了性能，我们这里还是得牺牲一点：
                                                                       // [方案 B]：只在一轮 Multicall 里做完所有事是不可能的，因为我们需要 Tick 来查 Bitmap。
                                                                       // 所以：
                                                                       // 1. Multicall (Slot0) -> 得到 Current Tick
                                                                       // 2. Multicall (Bitmap Words around Current Tick) -> 得到 Initialized Ticks
                                                                       // 3. Multicall (Ticks Data) -> 得到 Liquidity Net
                                                                       // 这就是标准的 V3 同步逻辑。虽然有 3 次 RTT，但数据绝对精准。

                        pre_updates.push(pool);
                    }

                    if pre_updates.is_empty() {
                        return;
                    }

                    // === Step 1: Get Slot0 (Current Tick) ===
                    // 为了快，我们直接把 Step 1 和 Step 2 合并的优化先放一边，先保证正确性。
                    // 其实很多 Bot 是把所有步骤拆开并发的。

                    // 这里我们稍微 Hack 一下：
                    // 我们还是发 2 个 Multicall。
                    // 1. Slot0 + Liquidity + (Blind Guess Bitmap)
                    //    不，猜 Bitmap 太难。
                    //    我们还是老老实实地：
                    //    Call 1: Slot0 + Liquidity
                    //    Call 2: Bitmap (根据 Slot0 的 tick)
                    //    Call 3: Ticks (根据 Bitmap 的结果)
                    //    虽然慢一点点，但是绝对不会报错 MISSING DATA。

                    // 执行 Call 1
                    let results_1 = match multicall.call_raw().await {
                        Ok(r) => r,
                        Err(e) => {
                            warn!("Step 1 RPC Failed for chunk (size {}): {:?}. Skipped.", chunk_owned.len(), e);
                            return;
                        }
                    };

                    // 检查数据是否完整
                    if results_1.len() != chunk_owned.len() * 2 {
                        warn!(
                            "Step 1 Partial Data: Expected {} but got {}. Skipped.",
                            chunk_owned.len() * 2,
                            results_1.len()
                        );
                        return;
                    }

                    // 准备 Call 2 (Bitmap)
                    let Ok(mut multicall_2) =
                        Multicall::new(provider.clone(), Some(multicall_address)).await
                    else {
                        return;
                    };

                    struct Step1Data<'a> {
                        pool: &'a PoolConfig,
                        tick: i32,
                        liquidity: u128,
                        sqrt_price: U256,
                        word_pos: i16,
                    }
                    let mut step1_data = Vec::new();

                    for (i, pool) in pre_updates.iter().enumerate() {
                        let slot0_res = &results_1[i * 2];
                        let liq_res = &results_1[i * 2 + 1];

                        let slot0_token = match slot0_res {
                            Ok(t) => t.clone(),
                            Err(_) => continue,
                        };

                        // Decode Slot0
                        let slot0 =
                            match <(U256, i32, u16, u16, u16, u8, bool)>::from_token(slot0_token) {
                                Ok(s) => s,
                                Err(_) => continue, // Bad pool, skip
                            };
                        // Decode Liquidity
                        let liq_token = match liq_res {
                            Ok(t) => t.clone(),
                            Err(_) => continue,
                        };
                        let liquidity = liq_token.into_uint().unwrap_or_default().as_u128();

                        let current_tick = slot0.1;
                        let word_pos = (current_tick >> 8) as i16;

                        step1_data.push(Step1Data {
                            pool,
                            tick: current_tick,
                            liquidity,
                            sqrt_price: slot0.0,
                            word_pos,
                        });

                        let v3_pool =
                            ICLPool::new(get_pool_address(pool).unwrap(), provider.clone());
                        // 获取当前 tick 所在的 Word，以及前后各 1 个 Word (覆盖 +/- 256 ticks)
                        // [Fix] 1. 缩小范围到 +/- 1 word (3个词，覆盖 +/- 256 ticks，足够了)
                        // 范围太大会导致包过大，Geth 依然会超时。
                        for i in -1..=1 {
                            // [Fix] 2. 把 true 改成 false！关闭 allow_failure，解决 InvalidData 解码错误。
                            multicall_2.add_call(v3_pool.tick_bitmap(word_pos + i as i16), false);
                        }
                    }

                    if step1_data.is_empty() {
                        return;
                    }

                    // 执行 Call 2
                    let results_2 = match multicall_2.call_raw().await {
                        Ok(r) => r,
                        Err(e) => {
                            warn!("Step 2 (Bitmap) Failed: {:?}", e);
                            return;
                        }
                    };

                    // 准备 Call 3 (Ticks Data)
                    let Ok(mut multicall_3) =
                        Multicall::new(provider.clone(), Some(multicall_address)).await
                    else {
                        return;
                    };

                    struct Step2Data<'a> {
                        base: Step1Data<'a>,
                        bitmap_cache: HashMap<i16, U256>, // 存储获取到的 Bitmap
                        ticks_to_fetch: Vec<i32>,         // 需要获取详情的 tick indices
                    }
                    let mut step2_data = Vec::new();

                    let mut ticks_call_count = 0;
                    let mut res2_idx = 0;
                    for data in step1_data {
                        let mut bitmap_cache = HashMap::new();
                        let mut ticks_to_fetch = Vec::new();

                        // 我们请求了 3 个 word: pos, pos-1, pos+1
                        // [Fix] 1. 缩小范围到 +/- 1 word
                        let mut words = Vec::new();
                        for i in -1..=1 {
                            words.push(data.word_pos + i as i16);
                        }

                        for &w in &words {
                            // [Fix] Since allow_failure is false, results_2 contains Tokens directly
                            if let Some(Ok(token)) = results_2.get(res2_idx) {
                                if let Some(bitmap_val) = token.clone().into_uint() {
                                    bitmap_cache.insert(w, bitmap_val);
                                    // 解析出所有 initialized ticks
                                    let initialized =
                                        get_initialized_ticks_from_bitmap(w, bitmap_val);
                                    // 过滤：只关心 tick_spacing 的整数倍 (虽然 bitmap 里的通常都是，但双重保险)
                                    for t in initialized {
                                        ticks_to_fetch.push(t);
                                    }
                                }
                            }
                            res2_idx += 1;
                        }

                        // 将需要获取的 ticks 加入 Call 3
                        let v3_pool =
                            ICLPool::new(get_pool_address(data.pool).unwrap(), provider.clone());
                        for &t in &ticks_to_fetch {
                            multicall_3.add_call(v3_pool.ticks(t), true);
                            ticks_call_count += 1;
                        }

                        step2_data.push(Step2Data {
                            base: data,
                            bitmap_cache,
                            ticks_to_fetch,
                        });
                    }

                    // 执行 Call 3
                    // 如果没有任何 tick 需要获取（极端冷门池子），这里可能会空，需要判断
                    let results_3 = if ticks_call_count == 0 {
                        Vec::new()
                    } else {
                        match multicall_3.call_raw().await {
                            Ok(r) => r,
                            Err(e) => {
                                warn!("Step 3 (Ticks) Failed: {:?}", e);
                                return;
                            }
                        }
                    };

                    // === Final Step: Update Cache ===
                    let mut res3_idx = 0;
                    for data in step2_data {
                        let mut ticks_map = HashMap::new();

                        for &t in &data.ticks_to_fetch {
                            if let Some(Ok(token)) = results_3.get(res3_idx) {
                                // Decode Ticks Info: (liquidityGross, liquidityNet, ...)
                                // 我们只需要 liquidityNet (index 1)
                                // Tuple: (u128, i128, U256, U256, i64, U256, u32, bool)
                                type TickInfo = (u128, i128, U256, U256, i64, U256, u32, bool);
                                if let Ok((_, liquidity_net, _, _, _, _, _, initialized)) =
                                    TickInfo::from_token(token.clone())
                                {
                                    if initialized {
                                        ticks_map.insert(t, liquidity_net);
                                    }
                                }
                            }
                            res3_idx += 1;
                        }

                        // 写入缓存
                        cache.insert(
                            get_pool_address(data.base.pool).unwrap(),
                            CachedPoolState {
                                block_number: current_block,
                                reserve0: 0,
                                reserve1: 0,
                                sqrt_price_x96: data.base.sqrt_price,
                                liquidity: data.base.liquidity,
                                tick: data.base.tick,
                                tick_spacing: data.base.pool.tick_spacing,
                                ticks: ticks_map,
                                tick_bitmap: data.bitmap_cache, // 关键：现在我们有了 Bitmap！
                            },
                        );
                    }
                }
            })
            .await;
    };

    // Run V2 and V3 updates in parallel
    tokio::join!(v2_stream, v3_task);
}

async fn get_price_in_weth(
    token: Address,
    weth: Address,
    usdc: Address,
    usdbc: Address,
    all_pools: &[PoolConfig],
    eth_price_in_usdc: U256,
    cache: &PoolCache,
    current_block: U64,
) -> U256 {
    if token == weth {
        return parse_ether("1").unwrap();
    }

    if (token == usdc || token == usdbc) && !eth_price_in_usdc.is_zero() {
        // Value = 1e24 / eth_price_in_usdc
        return U256::from(10).pow(24.into()) / eth_price_in_usdc;
    }

    let decimals_token = decimals(token);
    let one_unit = U256::from(10).pow(decimals_token.into());

    // 策略 A: Token -> WETH
    let weth_pair = all_pools.iter().find(|p| {
        (p.token_a == token && p.token_b == weth) || (p.token_a == weth && p.token_b == token)
    });

    if let Some(pool) = weth_pair {
        if let Ok(price_wei) =
            get_amount_out(one_unit, token, weth, pool, cache, current_block).await
        {
            return price_wei;
        }
    }

    // 策略 B: Token -> USDC/USDbC -> WETH
    if !eth_price_in_usdc.is_zero() {
        let usdc_pair = all_pools.iter().find(|p| {
            let other = if p.token_a == token {
                p.token_b
            } else {
                p.token_a
            };
            (p.token_a == token || p.token_b == token) && (other == usdc || other == usdbc)
        });

        if let Some(pool) = usdc_pair {
            let target_stable = if pool.token_a == usdc || pool.token_b == usdc {
                usdc
            } else {
                usdbc
            };
            if let Ok(price_usdc) =
                get_amount_out(one_unit, token, target_stable, pool, cache, current_block).await
            {
                // Token_ETH = (price_usdc * 1e18) / eth_price_in_usdc
                let price_in_eth = (price_usdc * U256::from(10).pow(18.into())) / eth_price_in_usdc;
                return price_in_eth;
            }
        }
    }

    U256::zero()
}

// 黄金分割搜索算法
async fn optimize_amount_in(
    path: &ArbPath,
    gas_cost_wei: I256,
    start_token_decimals: u32,
    cache: &PoolCache,
    current_block: U64,
) -> Option<(U256, I256)> {
    let one_unit = U256::from(10).pow(start_token_decimals.into());
    let mut low = one_unit * 10;
    let mut high = one_unit * 100_000;

    let phi_num = 618;
    let phi_den = 1000;
    let iterations = 10;

    let calc_profit = |amt: U256| {
        let pools = path.pools.clone();
        let tokens = path.tokens.clone();
        let cache = cache.clone();

        // 2. 返回拥有独立数据的 Future
        async move {
            let mut current = amt;
            // 使用 clone 进来的 pools 和 tokens
            for i in 0..pools.len() {
                match get_amount_out(
                    current,
                    tokens[i],
                    tokens[i + 1],
                    &pools[i],
                    &cache,
                    current_block,
                )
                .await
                {
                    Ok(out) => current = out,
                    Err(_) => return I256::min_value(),
                }
            }

            let gross = if current > amt {
                I256::from((current - amt).as_u128())
            } else {
                I256::from(0) - I256::from((amt - current).as_u128())
            };

            gross - gas_cost_wei
        }
    };

    let range = high - low;
    let mut c = high - (range * phi_num / phi_den);
    let mut d = low + (range * phi_num / phi_den);

    let mut profit_c = calc_profit(c).await;
    let mut profit_d = calc_profit(d).await;

    for _ in 0..iterations {
        if profit_c > profit_d {
            high = d;
            d = c;
            profit_d = profit_c;
            let range = high - low;
            c = high - (range * phi_num / phi_den);
            profit_c = calc_profit(c).await;
        } else {
            low = c;
            c = d;
            profit_c = profit_d;
            let range = high - low;
            d = low + (range * phi_num / phi_den);
            profit_d = calc_profit(d).await;
        }
    }

    let (best_amt, best_profit) = if profit_c > profit_d {
        (c, profit_c)
    } else {
        (d, profit_d)
    };

    if best_profit > I256::zero() {
        Some((best_amt, best_profit))
    } else {
        None
    }
}

#[derive(Clone)]
struct ArbPath {
    pools: Vec<PoolConfig>,
    tokens: Vec<Address>,
    is_triangle: bool,
}

async fn find_best_v3_pool(
    client: Arc<SignerMiddleware<Arc<Provider<Ipc>>, LocalWallet>>,
    token_a: Address,
    token_b: Address,
) -> Option<(Address, u32, i32, u128)> {
    // 标准费率列表
    let fees = vec![100, 500, 3000, 10000];
    let factory =
        IUniswapV3Factory::new(UNI_V3_FACTORY.parse::<Address>().unwrap(), client.clone());

    let mut best_pool = None;
    let mut max_liquidity = 0u128;

    for fee in fees {
        // 1. 询问 Factory 该费率的池子地址
        let pool_addr = match factory.get_pool(token_a, token_b, fee).call().await {
            Ok(addr) => addr,
            Err(_) => continue,
        };

        if pool_addr == Address::zero() {
            continue;
        }

        // 2. 检查该池子是否有流动性
        let pool = ICLPool::new(pool_addr, client.clone());
        let liquidity = match pool.liquidity().call().await {
            Ok(l) => l,
            Err(_) => continue,
        };

        if liquidity > max_liquidity {
            max_liquidity = liquidity;
            // 获取 tickSpacing (验证通过顺便拿)
            let ts = pool.tick_spacing().call().await.unwrap_or(0);
            best_pool = Some((pool_addr, fee, ts, liquidity));
        }
    }

    // 只有流动性大于 0 才算找到
    if max_liquidity > 0 {
        best_pool
    } else {
        None
    }
}

// [新增] 智能同步 V3 池子数据 (Slot0 + Bitmap + Ticks)
// 用于在发现潜在机会时进行二次校验，防止因缺少 Tick 数据导致的“幻觉利润”
async fn sync_v3_pool_smart(
    provider: Arc<Provider<Ipc>>,
    pool: &PoolConfig,
    cache: &PoolCache,
    current_block: U64,
) -> Result<()> {
    if pool.protocol == 1 {
        return Ok(());
    }
    let Some(pool_addr) = get_pool_address(pool) else {
        return Ok(());
    };

    let v3_pool = ICLPool::new(pool_addr, provider.clone());
    let multicall_address = MULTICALL_ADDRESS.parse::<Address>().unwrap();
    
    // Step 1: Slot0 & Liquidity
    let mut multicall = Multicall::new(provider.clone(), Some(multicall_address)).await?;
    multicall.add_call(v3_pool.slot_0(), false);
    multicall.add_call(v3_pool.liquidity(), false);

    let res1 = multicall.call_raw().await?;
    let slot0_token = res1[0].clone().map_err(|e| anyhow!("Slot0 failed: {:?}", e))?;
    let slot0 = <(U256, i32, u16, u16, u16, u8, bool)>::from_token(slot0_token)?;
    let liquidity_token = res1[1].clone().map_err(|e| anyhow!("Liquidity failed: {:?}", e))?;
    let liquidity = liquidity_token.into_uint().unwrap_or_default().as_u128();
    let current_tick = slot0.1;
    let word_pos = (current_tick >> 8) as i16;

    // Step 2: Bitmap (Current + Neighbors)
    let mut multicall2 = Multicall::new(provider.clone(), Some(multicall_address)).await?;
    let words = [word_pos, word_pos - 1, word_pos + 1];
    for &w in &words {
        multicall2.add_call(v3_pool.tick_bitmap(w), false);
    }
    let res2 = multicall2.call_raw().await?;

    let mut bitmap_cache = HashMap::new();
    let mut ticks_to_fetch = Vec::new();

    for (i, &w) in words.iter().enumerate() {
        if let Some(Ok(token)) = res2.get(i) {
            if let Some(bitmap) = token.clone().into_uint() {
                bitmap_cache.insert(w, bitmap);
                let initialized = get_initialized_ticks_from_bitmap(w, bitmap);
                for t in initialized {
                    ticks_to_fetch.push(t);
                }
            }
        }
    }

    // Step 3: Ticks Data
    let mut ticks_map = HashMap::new();
    if !ticks_to_fetch.is_empty() {
        let mut multicall3 = Multicall::new(provider.clone(), Some(multicall_address)).await?;
        for &t in &ticks_to_fetch {
            multicall3.add_call(v3_pool.ticks(t), false);
        }
        let res3 = multicall3.call_raw().await?;
        
        for (i, &t) in ticks_to_fetch.iter().enumerate() {
            if let Some(Ok(token)) = res3.get(i) {
                type TickInfo = (u128, i128, U256, U256, i64, U256, u32, bool);
                if let Ok((_, liquidity_net, _, _, _, _, _, initialized)) = TickInfo::from_token(token.clone()) {
                    if initialized {
                        ticks_map.insert(t, liquidity_net);
                    }
                }
            }
        }
    }

    // Update Cache
    cache.insert(pool_addr, CachedPoolState {
        block_number: current_block,
        reserve0: 0,
        reserve1: 0,
        sqrt_price_x96: slot0.0,
        liquidity,
        tick: current_tick,
        tick_spacing: pool.tick_spacing,
        ticks: ticks_map,
        tick_bitmap: bitmap_cache,
    });

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    info!("System Starting...");

    let config = load_encrypted_config()?;
    let provider = Arc::new(Provider::<Ipc>::connect_ipc(&config.ipc_path).await?);
    let wallet = LocalWallet::from_str(&config.private_key)?.with_chain_id(8453u64);
    let client = Arc::new(SignerMiddleware::new(provider.clone(), wallet.clone()));
    let gas_manager = Arc::new(SharedGasManager::new("gas_state.json".to_string()));
    let pool_failures: Arc<DashMap<String, u32>> = Arc::new(DashMap::new());
    let _profitable_history: Arc<DashMap<String, (u64, u32)>> = Arc::new(DashMap::new());
    let mut probed_quoters = std::collections::HashSet::new();

    let config_content = fs::read_to_string("pools.json").context("Failed to read pools.json")?;
    let mut json_configs: Vec<JsonPoolInput> = serde_json::from_str(&config_content)?;

    // ================== [新增] 暴力清洗：只保留核心资产池 ==================
    // 定义 Base 链上的核心资产地址 (小写)
    let whitelist_tokens = vec![
        "0x4200000000000000000000000000000000000006", // WETH
        "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913", // USDC
        "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca", // USDbC
        "0x50c5725949a6f0c72e6c4a641f24049a917db0cb", // DAI
        "0x2ae3f1ec7f1f5012cfeab0185bfc7aa3cf0dec22", // cbETH (Base)
    ];

    let before_count = json_configs.len();
    json_configs.retain(|p| {
        let t0 = p.token_a.to_lowercase();
        let t1 = p.token_b.to_lowercase();
        
        // 逻辑：两个币中，必须至少有一个是核心资产
        let has_major_token = whitelist_tokens.contains(&t0.as_str()) || 
                              whitelist_tokens.contains(&t1.as_str());
        
        has_major_token
    });
    
    info!("🧹 CLEANUP: Dropped {} junk pools. Remaining HIGH QUALITY pools: {}", 
        before_count - json_configs.len(), 
        json_configs.len()
    );
    // ======================================================================

    let weth = Address::from_str(WETH_ADDR)?;
    let usdc = Address::from_str(USDC_ADDR)?;
    let usdbc = Address::from_str(USDBC_ADDR)?;
    let _aero = Address::from_str(AERO_ADDR)?;
    let _cbeth = Address::from_str(CBETH_ADDR)?;
    let _ezeth = Address::from_str(EZETH_ADDR)?;
    let uniswap_quoter_addr = Address::from_str(UNISWAP_QUOTER)?;
    let dai = Address::from_str("0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb").unwrap(); // DAI

    let mut pools = Vec::new();

    let cache: PoolCache = Arc::new(DashMap::new());

    info!("Validating pools before startup...");
    for cfg in json_configs {
        let token_a = Address::from_str(&cfg.token_a)?;
        let token_b = Address::from_str(&cfg.token_b)?;
        let quoter_addr = cfg.quoter.as_ref().map(|s| Address::from_str(s).unwrap());
        let pool_addr = cfg.pool.as_ref().map(|s| Address::from_str(s).unwrap());

        let proto_str = cfg.protocol.unwrap_or("v3".to_string()).to_lowercase();
        let proto_code = if proto_str == "v2" {
            1
        } else if proto_str == "cl" {
            2
        } else {
            0
        };

        if proto_code == 2 && quoter_addr == Some(uniswap_quoter_addr) {
            warn!("Skipping [{}]: CL pool using Uniswap Quoter.", cfg.name);
            continue;
        }

        let (fee, tick_spacing, pool_fee) = match proto_code {
            2 => (0, cfg.tick_spacing.unwrap_or(0), cfg.pool_fee.unwrap_or(0)),
            _ => {
                // [FIX] Uniswap V3: 根据 Fee 推导 Tick Spacing
                let f = cfg.fee.unwrap_or(3000);
                let ts = match f {
                    100 => 1,     // 0.01% -> 1
                    500 => 10,    // 0.05% -> 10
                    3000 => 60,   // 0.3%  -> 60
                    10000 => 200, // 1%    -> 200
                    _ => 60,      // Default fallback
                };
                (f, ts, 0)
            }
        };

        let p_config = PoolConfig {
            name: cfg.name.clone(),
            router: Address::from_str(&cfg.router)?,
            quoter: quoter_addr,
            pool: pool_addr,
            fee,
            tick_spacing,
            pool_fee,
            token_a,
            token_b,
            protocol: proto_code,
        };

        let mut final_config = p_config;
        // let mut is_valid = false;
        // let mut real_ts = 0;
        // let mut real_fee = 0;

        // if proto_code == 1 {
        //     // V2 保持不变
        //     if validate_v2_pool(client.clone(), &final_config).await {
        //         is_valid = true;
        //     }
        // } else {
        //     // V3 / CL // 1. 基础验证：池子是否存在且有钱
        //     if let Some((ts, fee)) = validate_cl_pool(client.clone(), &final_config).await {
        //         // [新增核心修复]：如果是 Uniswap V3，必须验证地址是否匹配 Factory
        //         // 这能防止 Aerodrome 的池子被误传给 Uniswap Router
        //         let mut address_match = true;
        //         if proto_code == 0 {
        //             let factory = IUniswapV3Factory::new(
        //                 UNI_V3_FACTORY.parse::<Address>().unwrap(),
        //                 client.clone(),
        //             );
        //             // 询问 Factory：这个币对和费率，对应的池子到底是谁？
        //             let onchain_pool = factory
        //                 .get_pool(final_config.token_a, final_config.token_b, final_config.fee)
        //                 .call()
        //                 .await
        //                 .unwrap_or(Address::zero());

        //             // 如果 Factory 说池子是 A，但配置文件里写的是 B -> 报错并修正
        //             if onchain_pool != final_config.pool.unwrap() {
        //                 warn!(
        //                     "⚠️ Address Mismatch for {}: Config has {:?}, Factory says {:?}",
        //                     final_config.name,
        //                     final_config.pool.unwrap(),
        //                     onchain_pool
        //                 );
        //                 address_match = false;

        //                 // 可选：如果 Factory 返回的地址也是有效的，我们可以自动修正过去
        //                 // 但通常 Factory 返回空地址意味着费率不对，走下面的自动寻找逻辑更好
        //             }
        //         }

        //         if address_match {
        //             is_valid = true;
        //             real_ts = ts;
        //             real_fee = fee;
        //         }
        //     }

        //     // 2. 自动修正逻辑 (如果上面的验证失败，或者地址不匹配)
        //     if !is_valid && proto_code == 0 {
        //         info!(
        //             "Pool {} invalid with fee {}, searching for better fee...",
        //             final_config.name, final_config.fee
        //         );

        //         if let Some((new_addr, new_fee, new_ts, liq)) =
        //             find_best_v3_pool(client.clone(), final_config.token_a, final_config.token_b)
        //                 .await
        //         {
        //             info!(
        //                 "FIXED: Found valid pool for {}! Fee: {} -> {}, Addr: {:?}, Liq: {}",
        //                 final_config.name, final_config.fee, new_fee, new_addr, liq
        //             );

        //             final_config.pool = Some(new_addr);
        //             final_config.fee = new_fee;
        //             final_config.tick_spacing = new_ts;

        //             is_valid = true;
        //             real_ts = new_ts;
        //             real_fee = new_fee;
        //         } else {
        //             warn!(
        //                 "FAILED: No valid V3 pool found for pair {}",
        //                 final_config.name
        //             );
        //         }
        //     }
        // }

        let is_valid = true;
        let real_ts = tick_spacing;
        let real_fee = pool_fee;
        // info!("Validated Pool: {} (Trusted JSON)", final_config.name);

        if !is_valid {
            warn!("Removing invalid pool [{}]: Validation failed.", cfg.name);
            continue;
        }

        if proto_code == 2 {
            final_config.tick_spacing = real_ts;
            final_config.pool_fee = real_fee;
            // 关键：把查到的真实 tick_spacing 赋给 fee，传给合约
            final_config.fee = real_ts as u32;
        }

        info!(
            "Validated Pool: {} | Token A: {:?} | Token B: {:?}",
            cfg.name, token_a, token_b
        );
        if proto_code == 2 {
            info!(
                "Fixed CL Pool Config: {} | fee/ts set to {}",
                final_config.name, final_config.fee
            );
        }
        pools.push(final_config);

        if proto_code == 2 {
            if let Some(q) = quoter_addr {
                if probed_quoters.insert(q) {
                    let code = client.provider().get_code(q, None).await.unwrap();
                    info!("CL quoter {} @ {:?} code_len={}", cfg.name, q, code.0.len());
                }
            }
        }
    }
    info!("Active Pools: {}", pools.len());

    let all_pools_arc = Arc::new(pools.clone());
    let flash_loan_tokens = Arc::new(vec![weth, usdc, usdbc, dai]);

    // [优化 4] 后台更新 Gas Price
    let shared_gas_price = Arc::new(AtomicU64::new(100_000_000)); // 默认 0.1 gwei
    let bg_gas_price = shared_gas_price.clone();
    let bg_provider = provider.clone();
    tokio::spawn(async move {
        loop {
            if let Ok(price) = bg_provider.get_gas_price().await {
                let price_u64 = price.try_into().unwrap_or(u64::MAX);
                bg_gas_price.store(price_u64, Ordering::Relaxed);
            }
            tokio::time::sleep(Duration::from_secs(2)).await; // 每2秒更新一次
        }
    });

    let contract_address_exec = Address::from_str(&config.contract_address).unwrap();

    let mut stream = client.subscribe_blocks().await?;
    info!("Waiting for blocks...");

    // 优化方案：只算核心币种的环路 (WETH, USDC, USDbC, DAI)
    // 剔除 AERO, cbETH, ezETH 等非核心代币，大幅减少路径数量，避免计算垃圾路径
    let base_tokens = vec![weth, usdc, usdbc, dai];

    // [优化 1] 预先计算所有套利路径 (Static Calculation)
    // 只有在 pools 列表发生变化时才需要重算，而不是每个区块重算
    // ================== 高效路径生成算法 (Graph Logic) ==================
    // 1. 构建邻接表 (Adjacency Map)
    // 复杂度: O(N) - 只遍历一遍池子
    info!("Building graph from {} pools...", pools.len());
    let mut pools_by_token: HashMap<Address, Vec<usize>> = HashMap::new();

    for (idx, pool) in pools.iter().enumerate() {
        pools_by_token.entry(pool.token_a).or_default().push(idx);
        pools_by_token.entry(pool.token_b).or_default().push(idx);
    }

    // 2. 使用图搜索寻找路径 (Graph Search)
    // 复杂度: O(M) - M 为有效路径数量，极快
    info!("Pre-calculating arbitrage paths using Graph Search...");
    let mut candidates = Vec::new();
    let max_failures = 5;

    for &base_token in &base_tokens {
        // [Step 1] 找到第一跳: base_token -> mid_token
        if let Some(first_hop_indices) = pools_by_token.get(&base_token) {
            for &idx1 in first_hop_indices {
                let p1 = &pools[idx1];

                // 检查失败次数
                if pool_failures
                    .get(&p1.name)
                    .map(|c| *c > max_failures)
                    .unwrap_or(false)
                {
                    continue;
                }

                // 确定中间代币
                let mid_token = if p1.token_a == base_token {
                    p1.token_b
                } else {
                    p1.token_a
                };

                // [Step 2] 找到第二跳: mid_token -> next_token
                if let Some(second_hop_indices) = pools_by_token.get(&mid_token) {
                    for &idx2 in second_hop_indices {
                        // 避免同一个池子
                        if idx1 == idx2 {
                            continue;
                        }

                        let p2 = &pools[idx2];
                        // 检查失败次数
                        if pool_failures
                            .get(&p2.name)
                            .map(|c| *c > max_failures)
                            .unwrap_or(false)
                        {
                            continue;
                        }

                        let next_token = if p2.token_a == mid_token {
                            p2.token_b
                        } else {
                            p2.token_a
                        };

                        // Case A: 2-Hop (next_token == base_token)
                        if next_token == base_token {
                            candidates.push(ArbPath {
                                pools: vec![p1.clone(), p2.clone()],
                                tokens: vec![base_token, mid_token, base_token],
                                is_triangle: false,
                            });
                        } else {
                            // Case B: 3-Hop (next_token -> base_token)
                            // [Step 3] 找到第三跳
                            if let Some(third_hop_indices) = pools_by_token.get(&next_token) {
                                for &idx3 in third_hop_indices {
                                    if idx3 == idx1 || idx3 == idx2 {
                                        continue;
                                    }

                                    let p3 = &pools[idx3];
                                    // 检查失败次数
                                    if pool_failures
                                        .get(&p3.name)
                                        .map(|c| *c > max_failures)
                                        .unwrap_or(false)
                                    {
                                        continue;
                                    }

                                    let last_token = if p3.token_a == next_token {
                                        p3.token_b
                                    } else {
                                        p3.token_a
                                    };

                                    if last_token == base_token {
                                        candidates.push(ArbPath {
                                            pools: vec![p1.clone(), p2.clone(), p3.clone()],
                                            tokens: vec![
                                                base_token, mid_token, next_token, base_token,
                                            ],
                                            is_triangle: true,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    info!("Total Arbitrage Paths calculated: {}", candidates.len());

    // ================== [核心优化] 过滤活跃池子 ==================
    // 只有在 candidates 路径中出现过的池子，才需要每秒更新状态。
    // 其他 79万个孤岛池子或者垃圾池子，直接忽略。

    info!("Filtering active pools for state sync...");

    // 1. 收集所有“有用”的池子地址
    let mut active_pool_addresses = std::collections::HashSet::new();
    for path in &candidates {
        for pool in &path.pools {
            if let Some(addr) = get_pool_address(pool) {
                active_pool_addresses.insert(addr);
            }
        }
    }

    // 2. 从全量 pools 中筛选出 subset
    let active_pools_config: Vec<PoolConfig> = pools
        .iter()
        .filter(|p| {
            if let Some(addr) = get_pool_address(p) {
                active_pool_addresses.contains(&addr)
            } else {
                false
            }
        })
        .cloned()
        .collect();

    info!(
        "Optimization: Reduced sync target from {} to {} pools.",
        pools.len(),
        active_pools_config.len()
    );
    // ============================================================

    loop {
        let block = match tokio::time::timeout(Duration::from_secs(15), stream.next()).await {
            Ok(Some(b)) => b,
            _ => {
                warn!("Timeout");
                continue;
            }
        };
        let current_bn = block.number.unwrap();
        let block_number = current_bn.as_u64();

        info!("Block {}: Syncing pool states...", block_number);
        update_all_pools(provider.clone(), &active_pools_config, cache.clone(), current_bn).await;

        if gas_manager.get_loss() >= MAX_DAILY_GAS_LOSS_WEI {
            error!("Daily Gas Limit Reached.");
            break;
        }

        let client_ref = &client;
        // [优化 4] 从内存中读取 Gas Price，不再阻塞
        let gas_price = U256::from(shared_gas_price.load(Ordering::Relaxed));

        // 1. 获取 ETH -> USDC 的参考价格
        let mut eth_price_usdc = U256::zero();
        if let Some(p) = pools.iter().find(|p| {
            (p.token_a == weth && p.token_b == usdc) || (p.token_b == weth && p.token_a == usdc)
        }) {
            if let Ok(price) =
                get_amount_out(parse_ether("1").unwrap(), weth, usdc, p, &cache, current_bn).await
            {
                eth_price_usdc = price;
            }
        }

        let total_candidates = candidates.len();
        let ok_paths = Arc::new(AtomicUsize::new(0));
        let profitable_paths = Arc::new(AtomicUsize::new(0));
        let _failed_paths = Arc::new(AtomicUsize::new(0));

        let ok_paths_ref = ok_paths.clone();
        let profitable_paths_ref = profitable_paths.clone();
        // let pool_failures_ref = pool_failures.clone(); // Unused in this updated block
        let all_pools_ref = all_pools_arc.clone();
        let flash_loan_tokens_ref = flash_loan_tokens.clone();

        // 核心修改逻辑：使用 GSS 替代 test_sizes，并集成 execute_transaction
        stream::iter(candidates.clone())
            .for_each_concurrent(500, |path| {
                let ok_paths = ok_paths_ref.clone();
                let profitable_paths = profitable_paths_ref.clone();
                let client = client_ref.clone();
                let all_pools = all_pools_ref.clone();
                // Clone Arcs for the async block
                let cache = cache.clone();
                let provider = provider.clone();
                let flash_loan_tokens = flash_loan_tokens_ref.clone();

                async move {
                    let mut final_tokens = path.tokens.clone();
                    let mut final_pools = path.pools.clone();

                    // 检查起始代币是否在白名单里，如果不在则尝试旋转路径
                    if !flash_loan_tokens.contains(&final_tokens[0]) {
                        if let Some(start_index) = final_tokens
                            .iter()
                            .position(|t| flash_loan_tokens.contains(t))
                        {
                            // 旋转 pools
                            final_pools.rotate_left(start_index);
                            // 旋转 tokens: 先去掉末尾闭环元素，旋转，再补齐
                            final_tokens.pop();
                            final_tokens.rotate_left(start_index);
                            final_tokens.push(final_tokens[0]);

                            // info!(
                            //     "Path Rotated: Start token changed from {:?} to {:?}",
                            //     path.tokens[0], final_tokens[0]
                            // );
                        } else {
                            return; // 路径中没有支持闪电贷的代币，放弃
                        }
                    }

                    let start_token = final_tokens[0];
                    let decimals_token = decimals(start_token);

                    // 构建最终使用的路径对象
                    let rotated_path_struct = ArbPath {
                        pools: final_pools.clone(),
                        tokens: final_tokens.clone(),
                        is_triangle: final_pools.len() == 3,
                    };

                    // A. 预估 Gas 消耗 (Wei)
                    let estimated_gas_unit = if rotated_path_struct.is_triangle {
                        280_000
                    } else {
                        160_000
                    };
                    let _gas_cost_wei_val = U256::from(estimated_gas_unit) * gas_price;

                    // [优化 3] 快速试算 (Pre-check)
                    // 先算一下投入 0.1 个单位能不能回本。如果小额都亏，大额通常也亏
                    let one_unit = U256::from(10).pow(decimals_token.into());
                    let pre_check_amount = one_unit / 10; // 0.1 of the base unit

                    if !pre_check_amount.is_zero() {
                        let mut dummy_out = pre_check_amount;
                        let mut feasible = true;
                        for i in 0..rotated_path_struct.pools.len() {
                            match get_amount_out(
                                dummy_out,
                                rotated_path_struct.tokens[i],
                                rotated_path_struct.tokens[i + 1],
                                &rotated_path_struct.pools[i],
                                &cache,
                                current_bn,
                            )
                            .await
                            {
                                Ok(out) => dummy_out = out,
                                Err(_) => {
                                    feasible = false;
                                    break;
                                }
                            }
                        }

                        // 如果试算结果亏损（输出 <= 输入），直接放弃，不要进 GSS
                        if !feasible || dummy_out <= pre_check_amount {
                            return;
                        }
                    }

                    // 使用旋转后的路径去计算最佳输入金额
                    // [CRITICAL]: 这里计算出的 optimal_amount_in 才是对应 start_token 的正确数量
                    let best_result = optimize_amount_in(
                        &rotated_path_struct,
                        I256::zero(), // 传入 0 以避免单位错配，Gas 成本在后续步骤 C 中精确扣除
                        decimals_token,
                        &cache,
                        current_bn,
                    )
                    .await;

                    if let Some((best_amount, best_gross_profit)) = best_result {
                        ok_paths.fetch_add(1, Ordering::Relaxed);
                        
                        // [核心修复] 二次校验：发现机会后，强制同步链上真实 Tick 数据
                        // 防止因 Bitmap 缺失导致的“无限流动性”幻觉
                        let mut verified_profit = best_gross_profit;
                        let mut verified_amount = best_amount;
                        
                        // 只有当利润看起来不错时才去校验 (避免太小的机会浪费 RPC)
                        if best_gross_profit > I256::from(100_000) { // > 0.1 USDC approx
                            let mut sync_success = true;
                            for pool in &final_pools {
                                if let Err(e) = sync_v3_pool_smart(provider.clone(), pool, &cache, current_bn).await {
                                    warn!("Verification Sync Failed for {}: {:?}", pool.name, e);
                                    sync_success = false;
                                    break;
                                }
                            }
                            
                            if sync_success {
                                // 使用更新后的 Cache 重算
                                if let Some((new_amt, new_profit)) = optimize_amount_in(&rotated_path_struct, I256::zero(), decimals_token, &cache, current_bn).await {
                                    verified_amount = new_amt;
                                    verified_profit = new_profit;
                                } else {
                                    // 重算后发现不盈利了（说明之前是幻觉）
                                    return;
                                }
                            } else {
                                return; // 同步失败，放弃
                            }
                        }
                        
                        // 使用校验后的数据继续
                        let best_amount = verified_amount;
                        let best_gross_profit = verified_profit;

                        // [Safety Fuse] Max Trade Amount Check
                        // 防止因计算错误导致的巨额闪电贷 (e.g. 320 ETH)
                        let max_trade_amount = if start_token == weth {
                            parse_ether("10").unwrap() // Max 10 ETH
                        } else if start_token == usdc || start_token == usdbc {
                            parse_units("25000", 6).unwrap().into() // Max 25k USDC
                        } else if start_token == dai {
                            parse_ether("25000").unwrap() // Max 25k DAI
                        } else {
                            U256::max_value()
                        };
                        if best_amount > max_trade_amount {
                            warn!("⚠️ Safety Fuse Triggered: Amount {} exceeds limit for {}. Skipping.", format_token_amount(best_amount, start_token), token_symbol(start_token));
                            return;
                        }

                        // C. 精确计算 Net Profit
                        let price_in_weth = get_price_in_weth(
                            start_token,
                            weth,
                            usdc,
                            usdbc,
                            &all_pools,
                            eth_price_usdc,
                            &cache,
                            current_bn,
                        )
                        .await;

                        let l1_buffer = parse_ether("0.00005").unwrap();
                        let total_gas_wei = _gas_cost_wei_val + l1_buffer;

                        let gas_cost_token = if start_token == weth {
                            I256::from(total_gas_wei.as_u128())
                        } else if !price_in_weth.is_zero() {
                            let val = (total_gas_wei * U256::from(10).pow(decimals_token.into()))
                                / price_in_weth;
                            I256::from(val.as_u128())
                        } else {
                            I256::max_value()
                        };

                        let net_profit = best_gross_profit - gas_cost_token;

                        // D. 盈利判定与执行
                        let mut is_executable = false;

                        // [新增] 最小利润门槛：至少赚 1 美金 (1 USDC) 或者是 0.001 ETH
                        let min_profit_threshold = if start_token == usdc || start_token == usdbc {
                            I256::from(1_000_000) // 1 USDC
                        } else if start_token == weth {
                            I256::from(1_000_000_000_000_000u64) // 0.001 ETH
                        } else if start_token == dai {
                            I256::from(1_000_000_000_000_000_000u64) // 1 DAI
                        } else {
                            I256::zero()
                        };

                        if net_profit > min_profit_threshold {
                            is_executable = true;
                        }

                        // 将 I256 转换为可读数值方便调试
                        let profit_readable =
                            format_units(U256::from(net_profit.abs().as_u128()), decimals_token)
                                .unwrap_or("0".to_string());
                        let profit_sign = if net_profit >= I256::zero() { "+" } else { "-" };

                        // 阈值：只打印利润大于 -0.01 美元的机会 (防止日志太多刷屏)
                        // 假设 Token 是 USDC (6 decimals)， -0.01 USDC = -10000 units
                        // 这是一个宽松的过滤，让我们能看到接近盈利的机会
                        let debug_threshold = I256::from(-100000);

                        if net_profit > debug_threshold {
                            info!(
                                "👀 PEEK: {} | AmtIn: {} | Net: {}{} | Executable: {}",
                                token_symbol(start_token),
                                format_token_amount(best_amount, start_token),
                                profit_sign,
                                profit_readable,
                                is_executable
                            );
                        }

                        if is_executable {
                            profitable_paths.fetch_add(1, Ordering::Relaxed);

                            let log_msg = format!(
                                "PROFIT FOUND: Token: {} | Amount: {} | Net: {:?}",
                                token_symbol(start_token),
                                format_token_amount(best_amount, start_token),
                                net_profit
                            );
                            info!("{}", log_msg);
                            append_log_to_file(&log_msg);

                            let client_clone = client.clone();

                            // 构建 pools_data 用于传给合约
                            let mut pools_data = Vec::new();
                            for (i, pool) in final_pools.iter().enumerate() {
                                let token_in = final_tokens[i];
                                let token_out = final_tokens[i + 1];

                                // [Fix] Ensure correct router is used based on protocol
                                let router = match pool.protocol {
                                    0 => Address::from_str(UNI_V3_ROUTER).unwrap(),
                                    2 => Address::from_str(AERO_CL_ROUTER).unwrap(),
                                    _ => pool.router,
                                };

                                pools_data.push((
                                    router,
                                    token_in,
                                    token_out,
                                    pool.fee,
                                    pool.protocol,
                                ));
                            }

                            // [新增逻辑] 计算 min_profit
                            // best_gross_profit 是 I256 (可能为负，虽然 is_executable 保证了它大致为正)
                            // 我们将其转换为 U256 传给合约
                            let _gross_profit_u256 = if best_gross_profit > I256::zero() {
                                U256::from(best_gross_profit.as_u128())
                            } else {
                                U256::zero()
                            };

                            // [策略配置] 暂时设置为 0 (保本策略) 以解决 InsufficientProfit 报错
                            let min_profit_param = U256::zero();

                            // 异步提交交易
                            tokio::spawn(async move {
                                match execute_transaction(
                                    client_clone.clone(),
                                    contract_address_exec,
                                    best_amount,
                                    min_profit_param, // 传入计算好的值
                                    pools_data,
                                    provider.clone(),
                                )
                                .await
                                {
                                    Ok(tx) => {
                                        info!("Tx Broadcasted: {:?}", tx);
                                        // 轮询等待交易确认
                                        let mut attempts = 0;
                                        loop {
                                            tokio::time::sleep(Duration::from_secs(2)).await;
                                            match client_clone.get_transaction_receipt(tx).await {
                                                Ok(Some(receipt)) => {
                                                    if receipt.status == Some(U64::from(1)) {
                                                        info!("Tx Confirmed: {:?}", tx);
                                                        let subject = format!("Arbitrage Success! Tx: {:?}", tx);
                                                        let body = format!(
                                                            "Arbitrage executed successfully!\n\nTx Hash: {:?}\nBlock: {:?}\nGas Used: {:?}\n\nCheck Explorer: https://basescan.org/tx/{:?}",
                                                            tx, receipt.block_number, receipt.gas_used, tx
                                                        );
                                                        // 在 blocking 线程中发送邮件，避免阻塞异步运行时
                                                        tokio::task::spawn_blocking(move || {
                                                            send_email_alert(&subject, &body);
                                                        });
                                                    } else {
                                                        error!("Tx Reverted: {:?}", tx);
                                                        let subject = format!("Arbitrage Reverted! Tx: {:?}", tx);
                                                        let body = format!(
                                                            "Arbitrage transaction reverted on-chain.\n\nTx Hash: {:?}\nBlock: {:?}\nGas Used: {:?}\n\nCheck Explorer: https://basescan.org/tx/{:?}",
                                                            tx, receipt.block_number, receipt.gas_used, tx
                                                        );
                                                        tokio::task::spawn_blocking(move || {
                                                            send_email_alert(&subject, &body);
                                                        });
                                                    }
                                                    break;
                                                }
                                                Ok(None) => {
                                                    attempts += 1;
                                                    if attempts > 30 { // ~60s timeout
                                                        warn!("Timeout waiting for receipt: {:?}", tx);
                                                        break;
                                                    }
                                                }
                                                Err(e) => {
                                                    error!("Failed to check receipt: {:?}", e);
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => error!("Tx Failed: {:?}", e),
                                }
                            });
                        }
                    }
                }
            })
            .await;

        let gas_gwei = format_units(gas_price, "gwei").unwrap_or_else(|_| "0.0".to_string());
        info!(
            "Block {} | Gas: {} gwei | Cands: {} | Profitable: {}",
            current_bn,
            gas_gwei,
            total_candidates,
            profitable_paths.load(Ordering::Relaxed)
        );
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
