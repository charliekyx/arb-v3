use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct AppConfig {
    pub private_key: String,
    pub ipc_path: String,
    pub contract_address: String,
    pub smtp_username: String,
    pub smtp_password: String,
    pub my_email: String,
}