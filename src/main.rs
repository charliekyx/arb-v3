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
        atomic::{AtomicUsize, Ordering},
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
        function slot0() external view returns (uint160 sqrtPriceX96, int24 tick, uint16 observationIndex, uint16 observationCardinality, uint16 observationCardinalityNext, bool unlocked)
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

    info!(
        "CL Pool {} ok | ts={} fee={} liq={}",
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
    let v3_pools: Vec<_> = pools.iter().filter(|p| p.protocol != 1).collect();
    let v3_task = async {
        // Initialize Multicall. It might need a specific address on some chains.
        // Base Mainnet uses the standard Multicall3 address.
        // https://basescan.org/address/0xcA11bde05977b3631167028862bE2a173976CA11#code
        let multicall_address = "0xcA11bde05977b3631167028862bE2a173976CA11"
            .parse::<Address>()
            .unwrap();
        let Ok(mut multicall) = Multicall::new(provider.clone(), Some(multicall_address)).await
        else {
            error!("Failed to create Multicall instance");
            return;
        };

        let mut pre_updates = Vec::new();

        struct PoolPreUpdateData<'a> {
            pool: &'a PoolConfig,
            word_pos: i16,
        }

        for pool in &v3_pools {
            let Some(address) = get_pool_address(pool) else {
                continue;
            };
            if cache
                .get(&address)
                .map_or(false, |s| s.block_number == current_block)
            {
                continue;
            }

            // Use cached tick to determine which bitmap words to fetch. Fallback to 0.
            let cached_tick = cache.get(&address).map_or(0, |s| s.value().tick);
            let word_pos = (cached_tick >> 8) as i16;

            let v3_pool = ICLPool::new(address, provider.clone());
            multicall.add_call(v3_pool.slot_0(), true);
            multicall.add_call(v3_pool.liquidity(), true);
            multicall.add_call(v3_pool.tick_bitmap(word_pos), true); // Current word
            multicall.add_call(v3_pool.tick_bitmap(word_pos.wrapping_add(1)), true); // Word above
            multicall.add_call(v3_pool.tick_bitmap(word_pos.wrapping_sub(1)), true); // Word below

            pre_updates.push(PoolPreUpdateData { pool, word_pos });
        }

        if pre_updates.is_empty() {
            return;
        }

        let results = match multicall.call_raw().await {
            Ok(r) => r,
            Err(e) => {
                warn!("V3 Multicall 1 (Slot0/Liq/Bitmaps) failed: {:?}", e);
                return;
            }
        };

        // Prepare Multicall 2 for Ticks and Bitmap
        let Ok(mut multicall2) = Multicall::new(provider.clone(), Some(multicall_address)).await
        else {
            error!("Failed to create Multicall 2");
            return;
        };

        struct PoolFinalUpdateData<'a> {
            pool: &'a PoolConfig,
            slot0: (U256, i32, u16, u16, u16, bool),
            liquidity: u128,
            bitmaps: HashMap<i16, U256>,
            ticks_to_fetch: Vec<i32>,
        }
        let mut final_updates = Vec::new();
        let mut total_ticks_to_fetch = 0;

        for (i, pre_update) in pre_updates.iter().enumerate() {
            let pool = pre_update.pool;
            let base_idx = i * 5; // 5 calls per pool in MC1

            let slot0_res = &results[base_idx];
            let liq_res = &results[base_idx + 1];
            let bitmap_res = &results[base_idx + 2];
            let bitmap_plus_1_res = &results[base_idx + 3];
            let bitmap_minus_1_res = &results[base_idx + 4];

            if let (Ok(slot0_token), Ok(liq_token)) = (slot0_res, liq_res) {
                if let (Ok(slot0), Some(liq_val)) = (
                    <(U256, i32, u16, u16, u16, bool)>::from_token(slot0_token.clone()),
                    liq_token.clone().into_uint(),
                ) {
                    let liquidity = liq_val.as_u128();
                    let current_tick = slot0.1;
                    let new_word_pos = (current_tick >> 8) as i16;

                    // Check if our pre-fetched bitmaps are still relevant
                    if (new_word_pos - pre_update.word_pos).abs() > 1 {
                        warn!("Pool {} tick moved across more than one word. Cached: {}, New: {}. Skipping tick fetch for this block.", pool.name, pre_update.word_pos, new_word_pos);
                        cache.insert(
                            get_pool_address(pool).unwrap(),
                            CachedPoolState {
                                block_number: current_block,
                                reserve0: 0,
                                reserve1: 0,
                                sqrt_price_x96: slot0.0,
                                tick: current_tick,
                                liquidity,
                                tick_spacing: pool.tick_spacing,
                                ticks: HashMap::new(), // No tick data
                                tick_bitmap: HashMap::new(),
                            },
                        );
                        continue;
                    }

                    let mut bitmaps = HashMap::new();
                    let mut all_ticks_in_bitmaps = Vec::new();

                    let word_positions = [
                        pre_update.word_pos,
                        pre_update.word_pos.wrapping_add(1),
                        pre_update.word_pos.wrapping_sub(1),
                    ];
                    let bitmap_results = [bitmap_res, bitmap_plus_1_res, bitmap_minus_1_res];

                    for (j, &word_pos) in word_positions.iter().enumerate() {
                        if let Ok(bitmap_token) = bitmap_results[j] {
                            if let Some(bitmap_val) = bitmap_token.clone().into_uint() {
                                bitmaps.insert(word_pos, bitmap_val);
                                all_ticks_in_bitmaps.extend(get_initialized_ticks_from_bitmap(
                                    word_pos, bitmap_val,
                                ));
                            }
                        }
                    }

                    let ticks_to_fetch: Vec<i32> = all_ticks_in_bitmaps;

                    let v3_pool = ICLPool::new(get_pool_address(pool).unwrap(), provider.clone());
                    for &t in &ticks_to_fetch {
                        multicall2.add_call(v3_pool.ticks(t), true);
                    }

                    total_ticks_to_fetch += ticks_to_fetch.len();
                    final_updates.push(PoolFinalUpdateData {
                        pool,
                        slot0,
                        liquidity,
                        bitmaps,
                        ticks_to_fetch,
                    });
                }
            }
        }

        if final_updates.is_empty() {
            return;
        }

        info!(
            "Fetching {} total ticks across {} pools in Multicall 2",
            total_ticks_to_fetch,
            final_updates.len()
        );

        let results2 = match multicall2.call_raw().await {
            Ok(r) => r,
            Err(e) => {
                warn!("V3 Multicall 2 (Ticks) failed: {:?}", e);
                return;
            }
        };

        // 3. Final Cache Update
        let mut res_idx = 0;
        for update in final_updates {
            let mut ticks_map = HashMap::new();
            for &t in &update.ticks_to_fetch {
                if let Some(Ok(token)) = results2.get(res_idx) {
                    type TickInfo = (u128, i128, U256, U256, i64, U256, u32, bool);
                    if let Ok((_, liquidity_net, _, _, _, _, _, initialized)) =
                        TickInfo::from_token(token.clone())
                    {
                        if initialized {
                            ticks_map.insert(t, liquidity_net);
                        }
                    }
                }
                res_idx += 1;
            }

            cache.insert(
                get_pool_address(update.pool).unwrap(),
                CachedPoolState {
                    block_number: current_block,
                    reserve0: 0,
                    reserve1: 0,
                    sqrt_price_x96: update.slot0.0, // from slot0
                    tick: update.slot0.1,           // from slot0
                    liquidity: update.liquidity,    // from liquidity()
                    tick_spacing: update.pool.tick_spacing,
                    ticks: ticks_map,
                    tick_bitmap: update.bitmaps,
                },
            );
        }
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
    let json_configs: Vec<JsonPoolInput> = serde_json::from_str(&config_content)?;
    let weth = Address::from_str(WETH_ADDR)?;
    let usdc = Address::from_str(USDC_ADDR)?;
    let usdbc = Address::from_str(USDBC_ADDR)?;
    let aero = Address::from_str(AERO_ADDR)?;
    let cbeth = Address::from_str(CBETH_ADDR)?;
    let ezeth = Address::from_str(EZETH_ADDR)?;
    let uniswap_quoter_addr = Address::from_str(UNISWAP_QUOTER)?;

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

        let (is_valid, real_ts, real_fee) = match proto_code {
            1 => (validate_v2_pool(client.clone(), &p_config).await, 0, 0),
            2 => {
                if let Some((ts, fee)) = validate_cl_pool(client.clone(), &p_config).await {
                    (true, ts, fee)
                } else {
                    (false, 0, 0)
                }
            }
            _ => (true, 0, 0),
        };

        if !is_valid {
            warn!("Removing invalid pool [{}]: Validation failed.", cfg.name);
            continue;
        }

        let mut final_config = p_config;
        if proto_code == 2 {
            final_config.tick_spacing = real_ts;
            final_config.pool_fee = real_fee;
        }
        pools.push(final_config);

        info!(
            "Validated Pool: {} | Token A: {:?} | Token B: {:?}",
            cfg.name, token_a, token_b
        );

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
    let contract_address_exec = Address::from_str(&config.contract_address).unwrap();

    let mut stream = client.subscribe_blocks().await?;
    info!("Waiting for blocks...");

    let base_tokens = vec![weth, usdc, usdbc, aero, cbeth, ezeth];

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
        update_all_pools(provider.clone(), &pools, cache.clone(), current_bn).await;

        if gas_manager.get_loss() >= MAX_DAILY_GAS_LOSS_WEI {
            error!("Daily Gas Limit Reached.");
            break;
        }

        let client_ref = &client;
        let gas_price = provider
            .get_gas_price()
            .await
            .unwrap_or(parse_ether("0.0000000001").unwrap());

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

        let mut candidates = Vec::new();
        let max_failures = 5;

        // 遍历所有基准代币，寻找以它为起点的套利路径 (保持原始逻辑不变)
        for &base_token in &base_tokens {
            // 2-Hop
            for i in 0..pools.len() {
                for j in 0..pools.len() {
                    if i == j {
                        continue;
                    }
                    if pool_failures
                        .get(&pools[i].name)
                        .map(|c| *c > max_failures)
                        .unwrap_or(false)
                        || pool_failures
                            .get(&pools[j].name)
                            .map(|c| *c > max_failures)
                            .unwrap_or(false)
                    {
                        continue;
                    }

                    if (pools[i].token_a == base_token || pools[i].token_b == base_token)
                        && (pools[j].token_a == base_token || pools[j].token_b == base_token)
                    {
                        let mid_i = if pools[i].token_a == base_token {
                            pools[i].token_b
                        } else {
                            pools[i].token_a
                        };
                        let mid_j = if pools[j].token_a == base_token {
                            pools[j].token_b
                        } else {
                            pools[j].token_a
                        };

                        if mid_i == mid_j {
                            candidates.push(ArbPath {
                                pools: vec![pools[i].clone(), pools[j].clone()],
                                tokens: vec![base_token, mid_i, base_token],
                                is_triangle: false,
                            });
                        }
                    }
                }
            }

            // 3-Hop
            for i in 0..pools.len() {
                let pa = &pools[i];
                if pa.token_a != base_token && pa.token_b != base_token {
                    continue;
                }
                let token_1 = if pa.token_a == base_token {
                    pa.token_b
                } else {
                    pa.token_a
                };

                for j in 0..pools.len() {
                    if i == j {
                        continue;
                    }
                    if pool_failures
                        .get(&pools[i].name)
                        .map(|c| *c > max_failures)
                        .unwrap_or(false)
                        || pool_failures
                            .get(&pools[j].name)
                            .map(|c| *c > max_failures)
                            .unwrap_or(false)
                    {
                        continue;
                    }
                    let pb = &pools[j];

                    if pb.token_a != token_1 && pb.token_b != token_1 {
                        continue;
                    }
                    let token_2 = if pb.token_a == token_1 {
                        pb.token_b
                    } else {
                        pb.token_a
                    };

                    if token_2 == base_token {
                        continue;
                    }

                    for k in 0..pools.len() {
                        if k == i || k == j {
                            continue;
                        }
                        if pool_failures
                            .get(&pools[k].name)
                            .map(|c| *c > max_failures)
                            .unwrap_or(false)
                        {
                            continue;
                        }
                        let pc = &pools[k];
                        let pc_has_token2 = pc.token_a == token_2 || pc.token_b == token_2;
                        let pc_has_base = pc.token_a == base_token || pc.token_b == base_token;

                        if pc_has_token2 && pc_has_base {
                            candidates.push(ArbPath {
                                pools: vec![pa.clone(), pb.clone(), pc.clone()],
                                tokens: vec![base_token, token_1, token_2, base_token],
                                is_triangle: true,
                            });
                        }
                    }
                }
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

        // 核心修改逻辑：使用 GSS 替代 test_sizes，并集成 execute_transaction
        stream::iter(candidates).for_each_concurrent(500, |path| {
                let ok_paths = ok_paths_ref.clone();
                let profitable_paths = profitable_paths_ref.clone();
                let client = client_ref.clone();
                let all_pools = all_pools_ref.clone();
                // Clone Arcs for the async block
                let cache = cache.clone();
                let provider = provider.clone();

                async move {
                    // 1. 定义支持闪电贷的代币 (白名单)
                    let flash_loan_tokens = vec![
                        Address::from_str("0x4200000000000000000000000000000000000006").unwrap(), // WETH
                        Address::from_str("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913").unwrap(), // USDC
                        Address::from_str("0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA").unwrap(), // USDbC
                        Address::from_str("0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb").unwrap(), // DAI
                    ];

                    let mut final_tokens = path.tokens.clone();
                    let mut final_pools = path.pools.clone();

                    // 2. 检查起始代币是否在白名单里，如果不在则尝试旋转路径
                    if !flash_loan_tokens.contains(&final_tokens[0]) {
                        if let Some(start_index) = final_tokens.iter().position(|t| flash_loan_tokens.contains(t)) {
                            // 旋转 pools
                            final_pools.rotate_left(start_index);
                            // 旋转 tokens: 先去掉末尾闭环元素，旋转，再补齐
                            final_tokens.pop();
                            final_tokens.rotate_left(start_index);
                            final_tokens.push(final_tokens[0]);
                            
                            info!("Path Rotated: Start token changed from {:?} to {:?}", path.tokens[0], final_tokens[0]);
                        } else {
                            return; // 路径中没有支持闪电贷的代币，放弃
                        }
                    }

                    let start_token = final_tokens[0];
                    let decimals_token = decimals(start_token);
                    
                    // 构建最终使用的路径对象
                    let final_path = ArbPath {
                        pools: final_pools.clone(),
                        tokens: final_tokens.clone(),
                        is_triangle: final_pools.len() == 3,
                    };

                    // A. 预估 Gas 消耗 (Wei)
                    let estimated_gas_unit = if final_path.is_triangle { 280_000 } else { 160_000 };
                    let _gas_cost_wei_val = U256::from(estimated_gas_unit) * gas_price;

                    // B. 黄金分割搜索最佳金额
                    let best_result = optimize_amount_in(&final_path, I256::zero(), decimals_token, &cache, current_bn).await;

                    if let Some((best_amount, best_gross_profit)) = best_result {
                        ok_paths.fetch_add(1, Ordering::Relaxed);

                        // C. 精确计算 Net Profit
                        let weth_addr = Address::from_str(WETH_ADDR).unwrap();

                        let price_in_weth = get_price_in_weth(
                            start_token,
                            weth_addr,
                            Address::from_str(USDC_ADDR).unwrap(),
                            Address::from_str(USDBC_ADDR).unwrap(),
                            &all_pools,
                            eth_price_usdc,
                            &cache,
                            current_bn,
                        )
                        .await;

                        let l1_buffer = parse_ether("0.00005").unwrap();
                        let total_gas_wei = _gas_cost_wei_val + l1_buffer;

                        let gas_cost_token = if start_token == weth_addr {
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
                        let min_profit_eth_threshold = parse_ether("0.0005").unwrap();

                        if !price_in_weth.is_zero() && net_profit > I256::zero() {
                            let net_eth = (U256::from(net_profit.as_u128()) * price_in_weth)
                                / U256::from(10).pow(decimals_token.into());

                            if net_eth >= min_profit_eth_threshold {
                                is_executable = true;
                            }
                        } else if (start_token == Address::from_str(USDC_ADDR).unwrap()
                            || start_token == Address::from_str(USDBC_ADDR).unwrap())
                            && net_profit > I256::from(1_500_000)
                        {
                            is_executable = true;
                        }

                        if is_executable {

                            profitable_paths.fetch_add(1, Ordering::Relaxed);

                            let log_msg = format!(
                                "PROFIT FOUND: Token: {} | Size: {} | Gross: {} | Net: {} | Gas: {}",
                                token_symbol(start_token),
                                format_token_amount(best_amount, start_token),
                                best_gross_profit,
                                net_profit,
                                gas_cost_token
                            );
                            info!("{}", log_msg);
                            append_log_to_file(&log_msg);

                            let client_clone = client.clone();

                            // 异步提交交易
                            tokio::spawn(async move {
                                // 2. [关键修复] 正确构建 pools_data，确保 token_in/out 跟随路径方向
                                let mut pools_data = Vec::new();
                                for (i, pool) in final_path.pools.iter().enumerate() {
                                    // final_path.tokens 是有序的路径节点 [A, B, C, A]
                                    // 第 i 跳是从 tokens[i] -> tokens[i+1]
                                    let token_in = final_path.tokens[i];
                                    let token_out = final_path.tokens[i + 1];

                                    pools_data.push((
                                        pool.router,
                                        token_in,   // 必须是动态确定的方向，不能是 pool.token_a
                                        token_out,  // 必须是动态确定的方向
                                        pool.fee,
                                        pool.protocol,
                                    ));
                                }

                                // 3. 调用执行模块
                                match execute_transaction(
                                    client_clone,
                                    contract_address_exec,
                                    best_amount,
                                    U256::zero(), // expected_gross
                                    pools_data,   // 传入修正后的数据
                                    provider.clone(),
                                )
                                .await
                                {
                                    Ok(tx) => info!("Tx Broadcasted: {:?}", tx),
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
