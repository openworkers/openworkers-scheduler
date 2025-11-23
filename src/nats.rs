use base64::engine::general_purpose::STANDARD;
use base64::engine::Engine;

pub async fn nats_connect() -> Result<async_nats::Client, async_nats::ConnectError> {
    let nats_servers = std::env::var("NATS_SERVERS").expect("NATS_SERVERS must be set");

    log::debug!("connecting to nats: {nats_servers}");

    match std::env::var("NATS_CREDENTIALS") {
        // No credentials provided
        Err(_) => async_nats::connect(nats_servers).await,
        // Empty string means no credentials
        Ok(ref s) if s.is_empty() => async_nats::connect(nats_servers).await,
        Ok(credentials) => {
            // Decode the base64 encoded credentials
            let credentials: Vec<u8> = STANDARD
                .decode(credentials)
                .expect("failed to decode credentials");

            let credentials =
                String::from_utf8(credentials).expect("failed to convert credentials to string");

            let options = async_nats::ConnectOptions::new()
                .credentials(&credentials)
                .expect("failed to create nats options");

            options.connect(nats_servers).await
        }
    }
}
