use cocoon::Cocoon;
use rpassword::read_password;
use serde::Serialize;
use std::fs::File;
use std::io::prelude::*;

// 必须与 main.rs 中的 AppConfig 结构保持字段一致
#[derive(Serialize, serde::Deserialize, Debug)]
struct AppConfig {
    private_key: String,
    ipc_path: String,
    contract_address: String,
    smtp_username: String,
    smtp_password: String,
    my_email: String,
}

fn prompt(label: &str) -> String {
    print!("{}: ", label);
    std::io::stdout().flush().unwrap();
    let mut input = String::new();
    std::io::stdin().read_line(&mut input).unwrap();
    input.trim().to_string()
}

fn prompt_hidden(label: &str) -> String {
    print!("{}: ", label);
    std::io::stdout().flush().unwrap();
    read_password().unwrap()
}

fn main() {
    println!("=== MEV Bot Secure Config Generator ===");
    println!("此工具将把你的私钥和配置加密保存为 'mev_bot.secure' 文件。");

    // 1. 收集敏感信息
    let private_key = prompt_hidden("请输入 Bot 私钥 (输入时不可见)");
    let ipc_path = prompt("请输入 IPC 路径 (例如 /app/geth_data/geth.ipc)");
    let contract_address = prompt("请输入刚才部署的合约地址 (0x...)");

    // 为了兼容性保留 SMTP 字段，如果你不发邮件，可以直接回车跳过
    println!("(以下邮件配置可选，如果不使用请直接回车)");
    let smtp_username = prompt("SMTP 用户名");
    let smtp_password = prompt_hidden("SMTP 密码");
    let my_email = prompt("接收通知的邮箱");

    let config = AppConfig {
        private_key,
        ipc_path,
        contract_address,
        smtp_username,
        smtp_password,
        my_email,
    };

    println!("\n--------------------------------------------------");
    println!("现在设置一个【强密码】来加密这个配置文件。");
    println!("每次启动 Bot 时，你都需要通过环境变量 CONFIG_PASS 提供这个密码。");

    let password = prompt_hidden("设置加密密码");
    let confirm = prompt_hidden("确认加密密码");

    if password != confirm {
        eprintln!("❌ 两次输入的密码不一致！程序退出。");
        std::process::exit(1);
    }

    // 2. 加密并保存
    let mut cocoon = Cocoon::new(password.as_bytes());
    let mut file = File::create("mev_bot.secure").unwrap();

    // 序列化结构体 -> 字节 -> 加密写入
    let config_bytes = serde_json::to_vec(&config).expect("Failed to serialize config");

    match cocoon.dump(config_bytes, &mut file) {
        Ok(_) => {
            println!("\n✅ 成功！加密配置文件已保存为 'mev_bot.secure'。");
            println!(
                "下一步：在 docker-compose.yml 或 .env 中设置 CONFIG_PASS=你的密码 即可启动。"
            );
        }
        Err(e) => println!("\n❌ 错误：保存文件失败: {:?}", e),
    }
}
