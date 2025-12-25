// src/bin/setup_config.rs
use cocoon::Cocoon;
use rpassword::read_password;
use serde::Serialize;
use std::fs::File;
use std::io::prelude::*;

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
    println!("This tool will encrypt your secrets into a file.");

    let private_key = prompt_hidden("Enter Private Key (will be hidden)");
    let ipc_path = prompt("Enter IPC Path (e.g., /var/lib/reth/reth.ipc)");
    let contract_address = prompt("Enter FlashLoan Contract Address");
    let smtp_username = prompt("Enter SMTP Username (Gmail)");
    let smtp_password = prompt_hidden("Enter SMTP Password");
    let my_email = prompt("Enter Recipient Email");

    let config = AppConfig {
        private_key,
        ipc_path,
        contract_address,
        smtp_username,
        smtp_password,
        my_email,
    };

    println!("\nNow set a strong password to encrypt this file.");
    println!("You will need to provide this password when starting the bot.");
    let password = prompt_hidden("Encryption Password");
    let confirm = prompt_hidden("Confirm Password");

    if password != confirm {
        eprintln!("Passwords do not match!");
        std::process::exit(1);
    }

    let mut cocoon = Cocoon::new(password.as_bytes());
    let mut file = File::create("mev_bot.secure").unwrap();

    // 1. 先把结构体序列化成字节
    let config_bytes = serde_json::to_vec(&config).expect("Failed to serialize config");

    // 2. 把字节传给 dump
    match cocoon.dump(config_bytes, &mut file) {
        Ok(_) => println!("\n[SUCCESS] Encrypted config saved to 'mev_bot.secure'."),
        Err(e) => println!("\n[ERROR] Failed to save config: {:?}", e),
    }
}
