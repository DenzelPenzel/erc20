use alloy::providers::{IpcConnect, Provider, ProviderBuilder, WsConnect};
use alloy::transports::http::reqwest::Url;

pub async fn get_provider(url: &Url) -> anyhow::Result<Box<dyn Provider>> {
    match url.scheme() {
        "http" | "https" => Ok(Box::new(ProviderBuilder::new().connect_http(url.clone()))),
        "ws" | "wss" => {
            let provider = ProviderBuilder::new()
                .connect_ws(WsConnect::new(url.to_string()))
                .await?;
            Ok(Box::new(provider))
        }
        "file" => {
            let path = url
                .to_file_path()
                .map_err(|()| anyhow::anyhow!("Invalid IPC path: {url}"))?;
            let provider = ProviderBuilder::new().connect_ipc(IpcConnect::new(path)).await?;
            Ok(Box::new(provider))
        }
        _ => Err(anyhow::anyhow!("Invalid URL scheme: {}", url.scheme())),
    }
}
