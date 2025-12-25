# ========================
# Stage 1: Builder
# ========================
FROM rust:1.85-slim-bookworm as builder

WORKDIR /usr/src/app

# Install build dependencies
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

# --- Caching Trick Start ---
# 1. 创建空项目来缓存依赖
RUN mkdir src && echo "fn main() {}" > src/main.rs
COPY Cargo.toml Cargo.lock ./

# 2. 编译依赖 (release模式)
RUN cargo build --release

# 3. 删除假构建产物
# 注意：Cargo 在 deps 目录里通常把横杠转为下划线，所以这里用 rust_arbitrage_searcher*
RUN rm -f target/release/deps/rust_arbitrage_searcher_v3*
# --- Caching Trick End ---

# 4. 复制真正的源码
COPY src ./src

# 5. 编译真正的二进制文件
# 生成的可执行文件名为 rust-arbitrage-searcher (保留横杠)
RUN cargo build --release

# ========================
# Stage 2: Runtime
# ========================
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*

# Copy the binary
# 注意：源文件是 rust-arbitrage-searcher，我们将它重命名为 bot 方便运行
COPY --from=builder /usr/src/app/target/release/rust-arbitrage-searcher_v3 /app/bot

# Use a non-root user
RUN useradd -m mevuser
USER mevuser

# Run the binary
CMD ["./bot_v3"]