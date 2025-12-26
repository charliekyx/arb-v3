use anyhow::{anyhow, Context, Result};
use chrono::Local;
use cocoon::Cocoon;
use ethers::abi::{encode, Token};
use ethers::{
    prelude::*,
    types::{Address, I256, U256},
    utils::{format_ether, format_units, parse_ether, parse_units},
};
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
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
    protocol: u8, // 0=V3, 1=V2, 2=CL
}

// --- ABI Definitions ---
abigen!(
    IQuoterV2,
    r#"[
        struct QuoteParams { address tokenIn; address tokenOut; uint256 amountIn; uint24 fee; uint160 sqrtPriceLimitX96; }
        function quoteExactInputSingle(QuoteParams params) external returns (uint256 amountOut, uint160 sqrtPriceX96After, uint32 initializedTicksCrossed, uint256 gasEstimate)
    ]"#;

    IAerodromeCLQuoter,
    r#"[
        struct CLQuoteParams { address tokenIn; address tokenOut; uint256 amountIn; int24 tickSpacing; uint160 sqrtPriceLimitX96; }
        function quoteExactInputSingle(CLQuoteParams params) external returns (uint256 amountOut, uint256 r1, uint256 r2, uint256 r3)
    ]"#;

    ICLPool,
    r#"[
        function tickSpacing() external view returns (int24)
        function fee() external view returns (uint24)
        function liquidity() external view returns (uint128)
        function slot0() external view returns (uint160 sqrtPriceX96, int24 tick, uint16 observationIndex, uint16 observationCardinality, uint16 observationCardinalityNext, bool unlocked)
        function token0() external view returns (address)
    ]"#;

    IAerodromePair,
    r#"[
        function getReserves() external view returns (uint256 reserve0, uint256 reserve1, uint256 blockTimestampLast)
        function token0() external view returns (address)
        function token1() external view returns (address)
        function getAmountOut(uint256 amountIn, address tokenIn) external view returns (uint256 amountOut)
    ]"#
);

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

    // 恢复：写入 Gas 状态到文件
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

// 恢复：追加日志到本地文件
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

async fn probe_quoter(
    provider: &Provider<Ipc>,
    quoter: Address,
    token_in: Address,
    token_out: Address,
    amount_in: U256,
    tick_spacing: i32,
    fee_u24: u32,
) -> Result<()> {
    // 常见候选：fee / tickSpacing，不同顺序，有的没有 sqrtPriceLimitX96
    let candidates = vec![
        "quoteExactInputSingle(address,address,uint24,uint256,uint160)",
        "quoteExactInputSingle(address,address,int24,uint256,uint160)",
        "quoteExactInputSingle(address,address,uint256,uint24,uint160)",
        "quoteExactInputSingle(address,address,uint256,int24,uint160)",
        "quoteExactInputSingle(address,address,uint24,uint256)",
        "quoteExactInputSingle(address,address,int24,uint256)",
    ];
    for sig in candidates {
        let selector = sel4(sig);
        // 逐个 signature 手工拼 args（注意：encode 的 Token 列表必须和 sig 参数个数一致）
        let args: Vec<Token> = match sig {
            "quoteExactInputSingle(address,address,uint24,uint256,uint160)" => vec![
                Token::Address(token_in),
                Token::Address(token_out),
                Token::Uint(U256::from(fee_u24)),
                Token::Uint(amount_in),
                Token::Uint(U256::zero()),
            ],
            "quoteExactInputSingle(address,address,int24,uint256,uint160)" => vec![
                Token::Address(token_in),
                Token::Address(token_out),
                Token::Int(I256::from(tick_spacing).into_raw()),
                Token::Uint(amount_in),
                Token::Uint(U256::zero()),
            ],
            "quoteExactInputSingle(address,address,uint256,uint24,uint160)" => vec![
                Token::Address(token_in),
                Token::Address(token_out),
                Token::Uint(amount_in),
                Token::Uint(U256::from(fee_u24)),
                Token::Uint(U256::zero()),
            ],
            "quoteExactInputSingle(address,address,uint256,int24,uint160)" => vec![
                Token::Address(token_in),
                Token::Address(token_out),
                Token::Uint(amount_in),
                Token::Int(I256::from(tick_spacing).into_raw()),
                Token::Uint(U256::zero()),
            ],
            "quoteExactInputSingle(address,address,uint24,uint256)" => {
                vec![
                    Token::Address(token_in),
                    Token::Address(token_out),
                    Token::Uint(U256::from(fee_u24)),
                    Token::Uint(amount_in),
                ]
            }
            "quoteExactInputSingle(address,address,int24,uint256)" => {
                vec![
                    Token::Address(token_in),
                    Token::Address(token_out),
                    Token::Int(I256::from(tick_spacing).into_raw()),
                    Token::Uint(amount_in),
                ]
            }
            _ => unreachable!(),
        };
        let mut data = Vec::new();
        data.extend_from_slice(&selector);
        data.extend_from_slice(&encode(&args));
        let tx = TransactionRequest::new().to(quoter).data(Bytes::from(data));
        match provider.call(&tx.into(), None).await {
            Ok(ret) => {
                info!(
                    "PROBE OK {} sel=0x{} ret_len={}",
                    sig,
                    hex::encode(selector),
                    ret.0.len()
                );
            }
            Err(e) => {
                // 这里把错误完整打出来（有时包含 revert data）
                warn!(
                    "PROBE ERR {} sel=0x{} err={:?}",
                    sig,
                    hex::encode(selector),
                    e
                );
            }
        }
    }
    Ok(())
}

async fn probe_quoter_tuple(
    provider: &Provider<Ipc>,
    quoter: Address,
    token_in: Address,
    token_out: Address,
    amount_in: U256,
    tick_spacing: i32,
    fee_u24: u32,
) -> Result<()> {
    let candidates = vec![
        (
            "quoteExactInputSingle((address,address,uint256,uint24,uint160))",
            vec![Token::Tuple(vec![
                Token::Address(token_in),
                Token::Address(token_out),
                Token::Uint(amount_in),
                Token::Uint(U256::from(fee_u24)),
                Token::Uint(U256::zero()),
            ])],
        ),
        (
            "quoteExactInputSingle((address,address,uint256,int24,uint160))",
            vec![Token::Tuple(vec![
                Token::Address(token_in),
                Token::Address(token_out),
                Token::Uint(amount_in),
                Token::Int(I256::from(tick_spacing).into_raw()),
                Token::Uint(U256::zero()),
            ])],
        ),
    ];
    for (sig, args) in candidates {
        let selector = sel4(sig);
        let mut data = Vec::new();
        data.extend_from_slice(&selector);
        data.extend_from_slice(&encode(&args));
        let tx = TransactionRequest::new().to(quoter).data(Bytes::from(data));
        match provider.call(&tx.into(), None).await {
            Ok(ret) => {
                info!(
                    "PROBE OK {} sel=0x{} ret_len={}",
                    sig,
                    hex::encode(selector),
                    ret.0.len()
                );
                info!("ret=0x{}", hex::encode(&ret.0));
            }
            Err(e) => warn!(
                "PROBE ERR {} sel=0x{} err={:?}",
                sig,
                hex::encode(selector),
                e
            ),
        }
    }
    Ok(())
}

async fn validate_cl_pool(
    client: Arc<SignerMiddleware<Arc<Provider<Ipc>>, LocalWallet>>,
    pool: &PoolConfig,
) -> Option<(i32, u32)> {
    let Some(pool_addr) = pool.pool else {
        return None;
    };

    // 1) 先确认地址上有没有代码
    match client.provider().get_code(pool_addr, None).await {
        Ok(code) if code.0.is_empty() => {
            warn!("❌ CL Pool {} has no code @ {:?}", pool.name, pool_addr);
            return None;
        }
        Err(e) => {
            warn!(
                "❌ CL Pool {} getCode failed @ {:?}: {:?}",
                pool.name, pool_addr, e
            );
            return None;
        }
        _ => {}
    }

    // 2) 调试 slot0 raw
    // let _ = debug_slot0_raw(client.provider(), pool_addr).await;

    // 3) 改用 tickSpacing/fee/liquidity 做验证
    let contract = ICLPool::new(pool_addr, client.clone());
    let ts = match contract.tick_spacing().call().await {
        Ok(v) => v,
        Err(e) => {
            warn!(
                "❌ CL Pool {} tickSpacing() failed @ {:?}: {:?}",
                pool.name, pool_addr, e
            );
            return None;
        }
    };
    let fee = match contract.fee().call().await {
        Ok(v) => v,
        Err(e) => {
            warn!(
                "❌ CL Pool {} fee() failed @ {:?}: {:?}",
                pool.name, pool_addr, e
            );
            return None;
        }
    };
    let liq = match contract.liquidity().call().await {
        Ok(v) => v,
        Err(e) => {
            warn!(
                "❌ CL Pool {} liquidity() failed @ {:?}: {:?}",
                pool.name, pool_addr, e
            );
            return None;
        }
    };

    info!(
        "✅ CL Pool {} ok | ts={} fee={} liq={}",
        pool.name, ts, fee, liq
    );
    Some((ts, fee))
}

async fn validate_v2_pool(
    client: Arc<SignerMiddleware<Arc<Provider<Ipc>>, LocalWallet>>,
    pool: &PoolConfig,
) -> bool {
    if let Some(pair_addr) = pool.quoter {
        let pair = IAerodromePair::new(pair_addr, client.clone());

        // 🔥 最终方案：只要 getReserves 能调通，说明它就是个 V2 池，直接放行
        // 不再测试 getAmountOut，因为本金太小或太大都可能导致它 revert
        match pair.get_reserves().call().await {
            Ok(_) => true,
            Err(e) => {
                warn!("❌ Pool {} failed getReserves: {:?}", pool.name, e);
                false
            }
        }
    } else {
        false
    }
}

async fn get_amount_out(
    client: Arc<SignerMiddleware<Arc<Provider<Ipc>>, LocalWallet>>,
    pool: &PoolConfig,
    token_in: Address,
    token_out: Address,
    amount_in: U256,
) -> Result<U256> {
    if !pool_supports(pool, token_in, token_out) {
        return Err(anyhow!(
            "Pool mismatch: {} cannot swap {:?} -> {:?}",
            pool.name,
            token_in,
            token_out
        ));
    }
    if pool.protocol == 1 {
        let pair = IAerodromePair::new(pool.quoter.unwrap(), client);
        // 只信链上给的结果，不信本地算的
        return pair
            .get_amount_out(amount_in, token_in)
            .call()
            .await
            .map_err(|e| anyhow!("V2 On-chain Quote Fail: {}", e));
    } else if pool.protocol == 2 {
        let q = pool
            .quoter
            .ok_or_else(|| anyhow!("CL missing quoter: {}", pool.name))?;
        let quoter = IAerodromeCLQuoter::new(q, client.clone());
        let params = i_aerodrome_cl_quoter::CLQuoteParams {
            token_in,
            token_out,
            amount_in,
            tick_spacing: pool.tick_spacing,
            sqrt_price_limit_x96: U256::zero(),
        };
        let (amount_out, _r1, _r2, _r3) = quoter
            .quote_exact_input_single(params)
            .call()
            .await
            .map_err(|e| anyhow!("CL Quoter call failed {}: {:?}", pool.name, e))?;
        return Ok(amount_out);
    } else {
        let quoter_addr = pool.quoter.ok_or(anyhow!("V3 missing quoter"))?;
        let quoter = IQuoterV2::new(quoter_addr, client);
        let params = i_quoter_v2::QuoteParams {
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

#[derive(Clone)]
struct ArbPath {
    pools: Vec<PoolConfig>,
    tokens: Vec<Address>,
    is_triangle: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    info!("🚀 System Starting: Base Bot V5.8 (File Logging Restored)");

    let config = load_encrypted_config()?;
    let provider = Arc::new(Provider::<Ipc>::connect_ipc(&config.ipc_path).await?);
    let wallet = LocalWallet::from_str(&config.private_key)?.with_chain_id(8453u64);
    let client = Arc::new(SignerMiddleware::new(provider.clone(), wallet.clone()));
    let gas_manager = Arc::new(SharedGasManager::new("gas_state.json".to_string()));
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

    info!("🔍 Validating pools before startup...");
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
            warn!("⚠️ Skipping [{}]: CL pool using Uniswap Quoter.", cfg.name);
            continue;
        }

        let (fee, tick_spacing, pool_fee) = match proto_code {
            2 => (0, cfg.tick_spacing.unwrap_or(0), cfg.pool_fee.unwrap_or(0)),
            _ => (cfg.fee.unwrap_or(3000), 0, 0),
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
            warn!(
                "❌ Removing invalid pool [{}]: Validation failed.",
                cfg.name
            );
            continue;
        }

        // Update config with real on-chain values for CL pools
        let mut final_config = p_config;
        if proto_code == 2 {
            final_config.tick_spacing = real_ts;
            final_config.pool_fee = real_fee;
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
    info!("✅ Active Pools: {}", pools.len());

    // --- Static Probe for CL Quoter ---
    // AERO/USDC CL: 100 USDC -> AERO
    /*
    let usdc = Address::from_str(USDC_ADDR)?;
    let aero = Address::from_str("0x940181a94A35A4569E4529A3CDfB74e38FD98631")?;
    let q = Address::from_str("0x254cf9e1e6e233aa1ac962cb9b05b2cfeaae15b0")?;
    info!("🔍 Starting Static Probe for CL Quoter...");
    probe_quoter(
        provider.as_ref(),
        q,
        usdc,
        aero,
        parse_amount("100", usdc), // 100 USDC
        100,                       // tickSpacing
        500,                       // fee
    )
    .await?;

    probe_quoter_tuple(
        provider.as_ref(),
        q,
        usdc,
        aero,
        parse_amount("100", usdc),
        100,
        500,
    )
    .await?;
    */

    let mut stream = client.subscribe_blocks().await?;
    info!("Waiting for blocks...");

    // 定义所有作为起点的“基准代币”
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

        if gas_manager.get_loss() >= MAX_DAILY_GAS_LOSS_WEI {
            error!("💀 Daily Gas Limit Reached.");
            break;
        }

        let client_ref = &client;
        let gas_price = provider
            .get_gas_price()
            .await
            .unwrap_or(parse_ether("0.0000000001").unwrap());
        let _gas_cost_2hop = (gas_price * U256::from(300_000)).as_u128();
        let _gas_cost_3hop = (gas_price * U256::from(450_000)).as_u128();

        let mut candidates = Vec::new();

        // 遍历所有基准代币，寻找以它为起点的套利路径
        for &base_token in &base_tokens {
            // 2-Hop
            // 逻辑：base -> mid -> base
            for i in 0..pools.len() {
                for j in 0..pools.len() {
                    if i == j {
                        continue;
                    }
                    // 池子 A 包含 base_token，池子 B 也包含 base_token
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

                        // 如果两个池子的中间币是同一种
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
            // 逻辑：base -> token1 -> token2 -> base
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
                    let pb = &pools[j];

                    // 必须包含 token_1
                    if pb.token_a != token_1 && pb.token_b != token_1 {
                        continue;
                    }
                    let token_2 = if pb.token_a == token_1 {
                        pb.token_b
                    } else {
                        pb.token_a
                    };

                    // 如果第 2 跳直接回到了 base_token，那是 2-hop，跳过
                    if token_2 == base_token {
                        continue;
                    }

                    for k in 0..pools.len() {
                        if k == i || k == j {
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
        let failed_paths = Arc::new(AtomicUsize::new(0));
        let ok_paths_ref = ok_paths.clone();
        let profitable_paths_ref = profitable_paths.clone();
        let failed_paths_ref = failed_paths.clone();

        // 核心逻辑：遍历 candidates 路径
        stream::iter(candidates)
            .for_each_concurrent(10, |path| {
                let ok_paths = ok_paths_ref.clone();
                let profitable_paths = profitable_paths_ref.clone();
                let failed_paths = failed_paths_ref.clone();
                async move {
                    let mut path_is_ok = false;
                    let mut path_is_profitable = false;
                    let mut best_gross = I256::from(i64::MIN);
                    let mut best_report = String::new();
                    let mut found_any = false;

                    let start_token = path.tokens[0];
                    let test_sizes = if decimals(start_token) == 6 {
                        vec![
                            parse_amount("1000", start_token),
                            parse_amount("5000", start_token),
                            parse_amount("10000", start_token),
                        ]
                    } else {
                        vec![
                            parse_amount("1", start_token),
                            parse_amount("5", start_token),
                            parse_amount("10", start_token),
                        ]
                    };

                    // 🔥 局部改动：针对每条路径，跑遍所有资金档位
                    for size in test_sizes {
                        let mut current_amt = size;
                        let mut step_results = Vec::new();
                        let mut failed = false;

                        // 逐跳获取报价
                        for i in 0..path.pools.len() {
                            match get_amount_out(
                                client_ref.clone(),
                                &path.pools[i],
                                path.tokens[i],
                                path.tokens[i + 1],
                                current_amt,
                            )
                            .await
                            {
                                Ok(out) => {
                                    step_results.push((
                                        current_amt,
                                        out,
                                        path.pools[i].name.clone(),
                                    ));
                                    current_amt = out;
                                }
                                Err(e) => {
                                    warn!(
                                        "⚠️ Path failed at {} (Size: {}): {:?}",
                                        path.pools[i].name,
                                        format_ether(size),
                                        e
                                    );
                                    failed = true;
                                    break;
                                }
                            }
                        }

                        if !failed {
                            // 🔥 修改：只要路径没有 revert (failed == false)，就视为“有效路径”并计数
                            // 这样 OkPaths 就会显示所有能跑通的路径数量，而不仅仅是盈利的
                            if !path_is_ok {
                                ok_paths.fetch_add(1, Ordering::Relaxed);
                                path_is_ok = true;
                            }

                            // 计算毛利
                            let gross = if current_amt > size {
                                I256::from((current_amt - size).as_u128())
                            } else {
                                -I256::from((size - current_amt).as_u128())
                            };

                            // 估算 Gas (2-hop 用 160k, 3-hop 用 280k)
                            let gas_used = if path.is_triangle { 280_000 } else { 160_000 };
                            let gas_cost = I256::from((gas_price * U256::from(gas_used)).as_u128());
                            let net = gross - gas_cost;

                            found_any = true;
                            if gross > best_gross {
                                best_gross = gross;
                                let route_name = if path.is_triangle {
                                    format!(
                                        "{}->{}->{}",
                                        path.pools[0].name, path.pools[1].name, path.pools[2].name
                                    )
                                } else {
                                    format!("{}->{}", path.pools[0].name, path.pools[1].name)
                                };
                                best_report = format!(
                                    "🧊 WATCH: {} | Best Size: {} | Gross: {} | Net: {} (Gas: {})",
                                    route_name,
                                    format_token_amount(size, path.tokens[0]),
                                    gross,
                                    net,
                                    gas_cost
                                );
                            }

                            // 只要毛利为正，就记录“证据”
                            if gross > I256::zero() {
                                if !path_is_profitable {
                                    profitable_paths.fetch_add(1, Ordering::Relaxed);
                                    path_is_profitable = true;
                                }

                                let mut report = format!(
                                    "--- Opportunity (Size: {} ETH) ---\n",
                                    format_ether(size)
                                );
                                for (idx, (inp, outp, p_name)) in step_results.iter().enumerate() {
                                    report.push_str(&format!(
                                        "  Step {}: {} -> {} via {}\n",
                                        idx + 1,
                                        format_token_amount(*inp, path.tokens[idx]),
                                        format_token_amount(*outp, path.tokens[idx + 1]),
                                        p_name
                                    ));
                                }
                                report
                                    .push_str(&format!("  Gross: {} | Net: {} WEI\n", gross, net));

                                info!("{}", report);
                                append_log_to_file(&report);
                            }
                        }
                    }

                    if !path_is_ok {
                        failed_paths.fetch_add(1, Ordering::Relaxed);
                    }

                    if found_any {
                        // 估算 Gas (2-hop 用 160k, 3-hop 用 280k)
                        let gas_used = if path.is_triangle { 280_000 } else { 160_000 };
                        let gas_cost = I256::from((gas_price * U256::from(gas_used)).as_u128());
                        let best_net = best_gross - gas_cost;

                        // 仅当最佳档位的净利 > -0.00005 ETH 时才显示（接近盈利）
                        if best_net > I256::from(-50_000_000_000_000i128) {
                            info!("{}", best_report);
                        }
                    }
                }
            })
            .await;

        let gas_gwei = format_units(gas_price, "gwei").unwrap_or_else(|_| "0.0".to_string());
        let ok_paths_count = ok_paths.load(Ordering::Relaxed);
        let profitable_paths_count = profitable_paths.load(Ordering::Relaxed);
        let failed_paths_count = failed_paths.load(Ordering::Relaxed);
        info!(
            "--- Block {} | Gas: {} gwei | Cands: {} -> Ok: {} -> Profitable: {} -> Failed: {} ---",
            current_bn,
            gas_gwei,
            total_candidates,
            ok_paths_count,
            profitable_paths_count,
            failed_paths_count
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
