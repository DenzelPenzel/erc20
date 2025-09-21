pub mod helpers;
pub mod provider;

use alloy::rpc::client::BatchRequest;
use alloy::rpc::types::Filter;
use alloy::sol_types::SolEvent;
use alloy::{
    primitives::{Address, U256, U512, aliases::I512},
    providers::{Provider},
    rpc::types::{Log},
    sol,
    transports::http::reqwest::Url,
    transports::{RpcError, TransportErrorKind},
};
use clap::Parser;
use futures::{StreamExt as _, stream};
use indicatif::{ProgressBar, ProgressStyle};
use std::time::Duration;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::fs;
use tokio::{
    fs::File,
    signal::ctrl_c,
    sync::Semaphore,
};

sol! {
    event Transfer(address indexed from, address indexed to, uint256 value);
}

#[derive(Clone, Debug, thiserror::Error)]
enum DownloadError {
    #[error("RPC error: {0}")]
    RpcError(Arc<RpcError<TransportErrorKind>>),

    #[error("Log missing block number at index {index_in_vec}")]
    MissingBlockNumber { index_in_vec: usize },

    #[error("Log missing log index for block {block}")]
    MissingLogIndex { block: u64 },

    #[error("Timeout error")]
    Timeout,
}

impl From<RpcError<TransportErrorKind>> for DownloadError {
    fn from(value: RpcError<TransportErrorKind>) -> Self {
        DownloadError::RpcError(Arc::new(value))
    }
}

struct JobResult {
    from: u64,
    to: u64,
    result: Result<Vec<TransferEvent>, DownloadError>,
}

struct TransferEvent {
    block: u64,
    log_index: u64,
    token: Address,
    from: Address,
    to: Address,
    value: U256,
}

#[derive(Default)]
struct BalanceSheet {
    // (token, account) -> (inflow, outflow)
    pub in_out_flow: HashMap<(Address, Address), (U256, U256)>,
    pub total_transfers: usize,
}

impl BalanceSheet {
    pub fn extend(&mut self, events: impl Iterator<Item = TransferEvent>) {
        for evt in events {
            self.in_out_flow
                .entry((evt.token, evt.to))
                .and_modify(|(inflow, _)| {
                    *inflow += evt.value;
                })
                .or_insert((evt.value, U256::ZERO));

            self.in_out_flow
                .entry((evt.token, evt.from))
                .and_modify(|(_, outflow)| {
                    *outflow += evt.value;
                })
                .or_insert((U256::ZERO, evt.value));

            self.total_transfers += 1;
        }
    }

    fn finish(self) -> HashMap<Address, HashMap<Address, I512>> {
        let mut balances: HashMap<Address, HashMap<Address, I512>> = HashMap::new();

        for ((token, account), (inflow, outflow)) in self.in_out_flow {
            let inflow = I512::from_raw(U512::from(inflow));
            let outflow = I512::from_raw(U512::from(outflow));
            let balance = inflow.saturating_sub(outflow);

            if balance.is_zero() {
                continue;
            }

            balances.entry(token).or_default().insert(account, balance);
        }

        balances
    }

    async fn finalize(self, output: &Path, from: u64, to: u64) -> anyhow::Result<()> {
        let balances = self.finish();

        for (token, accounts) in &balances {
            let csv_path = output.join(format!("{token}_{from}_{to}.csv"));
            let file = File::create(csv_path).await?;
            let mut writer = csv_async::AsyncWriter::from_writer(file);
            
            writer.write_record(vec!["account", "balance"]).await?;

            for (&account, balance) in accounts {
                writer
                    .write_record(&[account.to_string(), balance.to_string()])
                    .await?;
            }
            
            writer.flush().await?;
        }

        Ok(())
    }
}

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "http://localhost:8545", env = "ETH_RPC_URL")]
    rpc_url: Url,

    #[arg(long, value_delimiter = ',')]
    addresses: Vec<Address>,

    #[arg(long, default_value = "0", help = "Starting block number, inclusive")]
    start_block: u64,

    #[arg(long, help = "Ending block number, inclusive. Default to latest")]
    end_block: Option<u64>,

    #[arg(long, default_value = "./data")]
    output_dir: PathBuf,

    #[arg(
        long,
        default_value = "1000",
        help = "Number of blocks to query in each eth_getLogs"
    )]
    chunk_size: u64,

    #[arg(
        long,
        default_value = "10",
        help = "Number of concurrent requests to make"
    )]
    concurrent_requests: usize,

    #[arg(long, help = "Debug mode")]
    debug: bool,

    #[arg(
        long,
        default_value = "30",
        help = "Timeout in seconds for each eth_getLogs"
    )]
    timeout: u64,

    #[arg(
        long,
        default_value = "10",
        help = "Number of retries for each eth_getLogs"
    )]
    retries: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subs = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subs)?;

    tracing::info!("Starting up");

    let args = Args::parse();

    let provider = provider::get_provider(&args.rpc_url)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to RPC: {}", e))?;

    let chain_id = provider
        .get_chain_id()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get chain ID: {}", e))?;

    if args.addresses.is_empty() {
        anyhow::bail!("No addresses provided");
    }

    let end_block = match args.end_block {
        Some(b) => b,
        None => provider
            .get_block_number()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get latest block number: {}", e))?,
    };

    if args.start_block > end_block {
        anyhow::bail!("Start block is greater than end block");
    }

    let chunks = helpers::build_chunks(args.start_block, end_block, args.chunk_size as u64)
        .into_iter()
        .rev()
        .collect::<Vec<_>>();

    tracing::info!("Chain ID: {}", chain_id);
    tracing::info!("Addresses: {:?}", args.addresses);
    tracing::info!("Start block: {}", args.start_block);
    tracing::info!("End block: {}", end_block);
    tracing::info!("Chunk size: {}", args.chunk_size);
    tracing::info!("Concurrent requests: {}", args.concurrent_requests);
    tracing::info!("Retries: {}", args.retries);

    fs::create_dir_all(&args.output_dir)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create output directory: {}", e))?;

    let pb = ProgressBar::new(chunks.len() as u64).with_style(
        ProgressStyle::default_bar()
            .template("{wide_bar} {pos}/{len} ETA:{eta} Elapsed:{elapsed}")
            .unwrap()
            .progress_chars("#>-"),
    );
    pb.tick();

    let mut balances = BalanceSheet::default();
    let mut failed_chunk = 0;
    let sem = Semaphore::new(args.concurrent_requests);

    tracing::info!("Downloading events");

    let mut res_stream = create_stream(&sem, &pb, &chunks, provider.as_ref(), &args);

    let done = loop {
        let job_res = tokio::select! {
            biased;
            _ = ctrl_c() => break false,
            r = res_stream.next() => r,
        };

        let Some(Some(job_res)) = job_res else {
            break true;
        };

        let res = match job_res.result {
            Ok(events) => events,
            Err(e) => {
                pb.println(format!(
                    "Error: processing block {}-{}: {:?}",
                    job_res.from, job_res.to, e
                ));
                failed_chunk += 1;
                continue;
            }
        };

        balances.extend(res.into_iter());
        pb.inc(1);
    };

    pb.finish_and_clear();

    tracing::info!("Total transfers: {}", balances.total_transfers);

    if !done {
        tracing::info!("Download interrupted by user");
        sem.close();
        if tokio::time::timeout(
            Duration::from_secs(args.timeout),
            sem.acquire_many(args.concurrent_requests as u32),
        )
        .await
        .is_err()
        {
            tracing::error!("Timeout while waiting for semaphore to be closed");
        };

        return Err(anyhow::anyhow!("Download interrupted by user"));
    }

    if failed_chunk > 0 {
        anyhow::bail!("Failed to download {} chunks", failed_chunk);
    } else {
        balances
            .finalize(&args.output_dir, args.start_block, end_block)
            .await?;
    }

    Ok(())
}

fn create_stream<'a>(
    sem: &'a Semaphore,
    pb: &'a ProgressBar,
    ranges: &'a [(u64, u64)],
    provider: &'a dyn Provider,
    args: &'a Args,
) -> impl futures::Stream<Item = Option<JobResult>> + 'a {
    let filter = Filter::default()
        .address(args.addresses.clone())
        .event_signature(Transfer::SIGNATURE_HASH);

    let &Args {
        timeout,
        concurrent_requests,
        debug,
        ..
    } = args;

    let timeout = Duration::from_secs(timeout);

    stream::iter(ranges.iter().copied().map(move |(from, to)| {
        let filter = filter.clone().from_block(from).to_block(to);
        let sem = sem;
        let provider = provider;

        async move {
            let Ok(_p) = sem.acquire().await else {
                return None;
            };

            if debug {
                pb.println(format!("Requesting blocks from {from} to {to}"));
            }

            let f = pull_logs(pb, provider, &filter, from, to);
            let res = match tokio::time::timeout(timeout, f).await {
                Ok(res) => res,
                Err(_) => Err(DownloadError::Timeout),
            };

            if debug {
                pb.println(format!("Requested blocks from {from} to {to}"));
            }

            Some(JobResult {
                from,
                to,
                result: res,
            })
        }
    }))
    .buffer_unordered(concurrent_requests)
}

async fn extract_events(
    provider: &dyn Provider,
    filter: &Filter,
    pb: &ProgressBar,
    from: u64,
    to: u64,
    block_size: usize,
) -> Result<Vec<Log>, DownloadError> {
    let logs = provider
        .get_logs(&filter.clone().from_block(from).to_block(to))
        .await;

    let err = match logs {
        Ok(logs) => return Ok(logs),
        Err(err) => err,
    };

    pb.println(format!(
        "Warning: eth_getLogs for blocks {from}-{to} failed because the chunk is too large, retrying with smaller chunks...",
    ));

    if err.as_error_resp().is_none_or(|resp| resp.code != -32602) {
        return Err(err.into());
    }

    let chunks = helpers::build_chunks(
        filter.get_from_block().unwrap(),
        filter.get_to_block().unwrap(),
        block_size as u64,
    );

    let mut batch = BatchRequest::new(provider.client());
    let mut waiters = Vec::with_capacity(chunks.len());

    for &(from, to) in &chunks {
        let filter = filter.clone().from_block(from).to_block(to);
        let waiter = batch
            .add_call::<_, Vec<Log>>("eth_getLogs", &[filter])
            .unwrap();

        waiters.push(waiter);
    }

    batch
        .await
        .map_err(|e| DownloadError::RpcError(Arc::new(e)))?;

    let mut logs = Vec::new();

    for waiter in waiters {
        logs.extend(waiter.await?);
    }

    Ok(logs)
}

const RETRY_BLOCK_SIZE: usize = 1;

async fn pull_logs<'a>(
    pb: &'a ProgressBar,
    provider: &'a dyn Provider,
    filter: &Filter,
    from: u64,
    to: u64,
) -> Result<Vec<TransferEvent>, DownloadError> {
    let logs = extract_events(provider, filter, pb, from, to, RETRY_BLOCK_SIZE).await?;
    let mut events = Vec::with_capacity(logs.len());

    for (i, log) in logs.into_iter().enumerate() {
        let Some(block) = log.block_number else {
            return Err(DownloadError::MissingBlockNumber { index_in_vec: i });
        };

        let Some(log_index) = log.log_index else {
            return Err(DownloadError::MissingLogIndex { block });
        };

        let Ok(e) = log.log_decode::<Transfer>() else {
            continue;
        };

        let event = TransferEvent {
            block,
            log_index,
            token: e.inner.address,
            from: e.inner.from,
            to: e.inner.to,
            value: e.inner.value,
        };

        events.push(event);
    }

    Ok(events)
}
