//  This is a modified version of bin/akula.rs from Akula.
//
//  Copyright 2022 Peter Davies
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use akula::{
    kv::{
        tables::{self, ErasedTable},
        traits::*,
    },
    models::*,
    stagedsync::{stage::*, stages::EXECUTION, stages::FINISH},
    stages::*,
    version_string, StageId,
};
use anyhow::{bail, Context};
use async_trait::async_trait;
use clap::Parser;
use derive_more::*;
use directories::ProjectDirs;
use rayon::prelude::*;
use std::{
    fmt::Display,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::pin;
use tokio_stream::StreamExt;
use tracing::*;
use tracing_subscriber::{prelude::*, EnvFilter};

mod execution;
mod snapshot;
mod stagedsync;

#[derive(Debug, Deref, DerefMut, FromStr)]
pub struct AkulaDataDir(pub PathBuf);

impl AkulaDataDir {
    pub fn chain_data_dir(&self) -> PathBuf {
        self.0.join("chaindata")
    }

    pub fn etl_temp_dir(&self) -> PathBuf {
        self.0.join("etl-temp")
    }
}

impl Default for AkulaDataDir {
    fn default() -> Self {
        Self(
            ProjectDirs::from("", "", "PyspecSnapshot")
                .map(|pd| pd.data_dir().to_path_buf())
                .unwrap_or_else(|| "data".into()),
        )
    }
}

impl Display for AkulaDataDir {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_os_str().to_str().unwrap())
    }
}

#[derive(Parser)]
#[clap(
    name = "Rust Pyspec Snapshot",
    about = "Generates state snapshots for the Ethereum Execution Specs"
)]
pub struct Opt {
    /// Path to Erigon database directory, where to get blocks from.
    #[clap(long = "erigon-datadir", parse(from_os_str))]
    pub erigon_data_dir: Option<PathBuf>,

    /// Path to Akula database directory.
    #[clap(long = "datadir", help = "Database directory path", default_value_t)]
    pub data_dir: AkulaDataDir,

    /// Name of the testnet to join
    #[clap(
        long = "chain",
        help = "Name of the testnet to join",
        default_value = "mainnet"
    )]
    pub chain_name: String,

    /// Downloader options.
    #[clap(flatten)]
    pub downloader_opts: akula::downloader::opts::Opts,

    /// Sender recovery batch size (blocks)
    #[clap(long, default_value = "50000")]
    pub sender_recovery_batch_size: u64,

    /// Execution batch size (Ggas).
    #[clap(long, default_value = "5000")]
    pub execution_batch_size: u64,

    /// Execution history batch size (Ggas).
    #[clap(long, default_value = "250")]
    pub execution_history_batch_size: u64,

    #[clap(long)]
    pub snapshot: BlockNumber,

    #[clap(short, long, parse(from_os_str))]
    pub output: Option<PathBuf>,
}

#[derive(Debug)]
struct ConvertHeaders<Source>
where
    Source: KV,
{
    db: Arc<Source>,
    max_block: Option<BlockNumber>,
}

#[async_trait]
impl<'db, RwTx, Source> Stage<'db, RwTx> for ConvertHeaders<Source>
where
    Source: KV,
    RwTx: MutableTransaction<'db>,
{
    fn id(&self) -> StageId {
        StageId("ConvertHeaders")
    }
    fn description(&self) -> &'static str {
        ""
    }
    async fn execute<'tx>(&self, tx: &'tx mut RwTx, input: StageInput) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        let original_highest_block = input.stage_progress.unwrap_or(BlockNumber(0));
        let mut highest_block = original_highest_block;

        let erigon_tx = self.db.begin().await?;

        let mut erigon_canonical_cur = erigon_tx.cursor(tables::CanonicalHeader).await?;
        let mut canonical_cur = tx.mutable_cursor(tables::CanonicalHeader).await?;
        let mut erigon_header_cur = erigon_tx.cursor(tables::Header).await?;
        let mut header_cur = tx.mutable_cursor(tables::Header).await?;
        let mut erigon_td_cur = erigon_tx.cursor(tables::HeadersTotalDifficulty).await?;
        let mut td_cur = tx.mutable_cursor(tables::HeadersTotalDifficulty).await?;

        if erigon_tx
            .get(tables::CanonicalHeader, highest_block)
            .await?
            != tx.get(tables::CanonicalHeader, highest_block).await?
        {
            return Ok(ExecOutput::Unwind {
                unwind_to: BlockNumber(highest_block.0 - 1),
            });
        }

        let walker = walk(&mut erigon_canonical_cur, Some(highest_block + 1));
        pin!(walker);
        while let Some((block_number, canonical_hash)) = walker.try_next().await? {
            if block_number > self.max_block.unwrap_or(BlockNumber(u64::MAX)) {
                break;
            }

            highest_block = block_number;

            canonical_cur.append(block_number, canonical_hash).await?;
            header_cur
                .append(
                    (block_number, canonical_hash),
                    erigon_header_cur
                        .seek_exact((block_number, canonical_hash))
                        .await?
                        .unwrap()
                        .1,
                )
                .await?;
            td_cur
                .append(
                    (block_number, canonical_hash),
                    erigon_td_cur
                        .seek_exact((block_number, canonical_hash))
                        .await?
                        .unwrap()
                        .1,
                )
                .await?;

            if block_number.0 % 500_000 == 0 {
                info!("Extracted block {}", block_number);
            }
        }

        Ok(ExecOutput::Progress {
            stage_progress: highest_block,
            done: true,
        })
    }

    async fn unwind<'tx>(
        &self,
        tx: &'tx mut RwTx,
        input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        let mut canonical_cur = tx.mutable_cursor(tables::CanonicalHeader).await?;

        while let Some((block_num, _)) = canonical_cur.last().await? {
            if block_num <= input.unwind_to {
                break;
            }

            canonical_cur.delete_current().await?;
        }

        let mut header_cur = tx.mutable_cursor(tables::Header).await?;
        while let Some(((block_num, _), _)) = header_cur.last().await? {
            if block_num <= input.unwind_to {
                break;
            }

            header_cur.delete_current().await?;
        }

        let mut td_cur = tx.mutable_cursor(tables::HeadersTotalDifficulty).await?;
        while let Some(((block_num, _), _)) = td_cur.last().await? {
            if block_num <= input.unwind_to {
                break;
            }

            td_cur.delete_current().await?;
        }

        Ok(UnwindOutput {
            stage_progress: input.unwind_to,
        })
    }
}

#[derive(Debug)]
struct ConvertBodies<Source>
where
    Source: KV,
{
    db: Arc<Source>,
}

#[async_trait]
impl<'db, RwTx, Source> Stage<'db, RwTx> for ConvertBodies<Source>
where
    Source: KV,
    RwTx: MutableTransaction<'db>,
{
    fn id(&self) -> StageId {
        StageId("ConvertBodies")
    }
    fn description(&self) -> &'static str {
        ""
    }
    #[allow(clippy::redundant_closure_call)]
    async fn execute<'tx>(&self, tx: &'tx mut RwTx, input: StageInput) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        let original_highest_block = input.stage_progress.unwrap_or(BlockNumber(0));
        let mut highest_block = original_highest_block;

        const MAX_TXS_PER_BATCH: usize = 500_000;
        const BUFFERING_FACTOR: usize = 500_000;
        let erigon_tx = self.db.begin().await?;

        if erigon_tx
            .get(tables::CanonicalHeader, highest_block)
            .await?
            != tx.get(tables::CanonicalHeader, highest_block).await?
        {
            return Ok(ExecOutput::Unwind {
                unwind_to: BlockNumber(highest_block.0 - 1),
            });
        }

        let mut canonical_header_cur = tx.cursor(tables::CanonicalHeader).await?;

        let mut erigon_body_cur = erigon_tx.cursor(tables::BlockBody).await?;
        let mut body_cur = tx.mutable_cursor(tables::BlockBody).await?;

        let mut erigon_tx_cur = erigon_tx.cursor(tables::BlockTransaction.erased()).await?;
        let mut tx_cur = tx.mutable_cursor(tables::BlockTransaction.erased()).await?;

        let prev_body = tx
            .get(
                tables::BlockBody,
                (
                    highest_block,
                    tx.get(tables::CanonicalHeader, highest_block)
                        .await?
                        .unwrap(),
                ),
            )
            .await?
            .unwrap();

        let mut starting_index = prev_body.base_tx_id + prev_body.tx_amount as u64;
        let canonical_header_walker = walk(&mut canonical_header_cur, Some(highest_block + 1));
        pin!(canonical_header_walker);
        let mut batch = Vec::with_capacity(BUFFERING_FACTOR);
        let mut converted = Vec::with_capacity(BUFFERING_FACTOR);

        let mut extracted_blocks_num = 0;
        let mut extracted_txs_num = 0;

        let started_at = Instant::now();
        let done = loop {
            let mut no_more_bodies = true;
            let mut accum_txs = 0;
            while let Some((block_num, block_hash)) = canonical_header_walker.try_next().await? {
                if let Some((_, body)) = erigon_body_cur.seek_exact((block_num, block_hash)).await?
                {
                    let base_tx_id = body.base_tx_id;

                    let txs = walk(&mut erigon_tx_cur, Some(base_tx_id.encode().to_vec()))
                        .map(|res| res.map(|(_, tx)| tx))
                        .take(body.tx_amount)
                        .collect::<anyhow::Result<Vec<_>>>()
                        .await?;

                    if txs.len() != body.tx_amount {
                        bail!(
                            "Invalid tx amount in Erigon for block #{}/{}: {} != {}",
                            block_num,
                            block_hash,
                            body.tx_amount,
                            txs.len()
                        );
                    }

                    accum_txs += body.tx_amount;
                    batch.push((block_num, block_hash, body, txs));

                    if accum_txs > MAX_TXS_PER_BATCH {
                        no_more_bodies = false;
                        break;
                    }
                } else {
                    break;
                }
            }

            extracted_blocks_num += batch.len();
            extracted_txs_num += accum_txs;

            batch
                .par_drain(..)
                .map(move |(block_number, block_hash, body, txs)| {
                    Ok::<_, anyhow::Error>((
                        block_number,
                        block_hash,
                        body.uncles,
                        txs.into_iter()
                            .map(|v| {
                                Ok(rlp::decode::<akula::models::MessageWithSignature>(&v)?
                                    .encode()
                                    .to_vec())
                            })
                            .collect::<anyhow::Result<Vec<_>>>()?,
                    ))
                })
                .collect_into_vec(&mut converted);

            for res in converted.drain(..) {
                let (block_num, block_hash, uncles, txs) = res?;
                highest_block = block_num;
                let body = BodyForStorage {
                    base_tx_id: starting_index,
                    tx_amount: txs.len(),
                    uncles,
                };

                body_cur.append((block_num, block_hash), body).await?;

                for tx in txs {
                    tx_cur
                        .append(
                            ErasedTable::<tables::BlockTransaction>::encode_key(starting_index)
                                .to_vec(),
                            tx,
                        )
                        .await?;
                    starting_index.0 += 1;
                }
            }

            if no_more_bodies {
                break true;
            }

            let now = Instant::now();
            let elapsed = now - started_at;
            if elapsed > Duration::from_secs(30) {
                info!(
                    "Highest block {}, batch size: {} blocks with {} transactions, {} tx/sec",
                    highest_block.0,
                    extracted_blocks_num,
                    extracted_txs_num,
                    extracted_txs_num as f64
                        / (elapsed.as_secs() as f64 + (elapsed.subsec_millis() as f64 / 1000_f64))
                );

                break no_more_bodies;
            }
        };

        Ok(ExecOutput::Progress {
            stage_progress: highest_block,
            done,
        })
    }
    async fn unwind<'tx>(
        &self,
        tx: &'tx mut RwTx,
        input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        let mut block_body_cur = tx.mutable_cursor(tables::BlockBody).await?;
        let mut block_tx_cur = tx.mutable_cursor(tables::BlockTransaction).await?;
        while let Some(((block_num, _), body)) = block_body_cur.last().await? {
            if block_num <= input.unwind_to {
                break;
            }

            block_body_cur.delete_current().await?;

            let mut deleted = 0;
            while deleted < body.tx_amount {
                let to_delete = body.base_tx_id + deleted.try_into().unwrap();
                if block_tx_cur.seek(to_delete).await?.is_some() {
                    block_tx_cur.delete_current().await?;
                }

                deleted += 1;
            }
        }

        Ok(UnwindOutput {
            stage_progress: input.unwind_to,
        })
    }
}

#[derive(Debug)]
struct TerminatingStage {
    max_block: Option<BlockNumber>,
    exit_after_sync: bool,
    delay_after_sync: Duration,
}

#[async_trait]
impl<'db, RwTx> Stage<'db, RwTx> for TerminatingStage
where
    RwTx: MutableTransaction<'db>,
{
    fn id(&self) -> StageId {
        FINISH
    }
    fn description(&self) -> &'static str {
        ""
    }
    async fn execute<'tx>(&self, _: &'tx mut RwTx, input: StageInput) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        let prev_stage = input
            .previous_stage
            .map(|(_, b)| b)
            .unwrap_or(BlockNumber(0));
        let last_cycle_progress = input.stage_progress.unwrap_or(BlockNumber(0));
        Ok(
            if prev_stage > last_cycle_progress
                && prev_stage < self.max_block.unwrap_or(BlockNumber(u64::MAX))
            {
                ExecOutput::Progress {
                    stage_progress: prev_stage,
                    done: true,
                }
            } else {
                if self.exit_after_sync {
                    info!("Sync complete, exiting.");
                    std::process::exit(0)
                }

                tokio::time::sleep(self.delay_after_sync).await;

                ExecOutput::Progress {
                    stage_progress: prev_stage,
                    done: true,
                }
            },
        )
    }
    async fn unwind<'tx>(
        &self,
        _: &'tx mut RwTx,
        input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        Ok(UnwindOutput {
            stage_progress: input.unwind_to,
        })
    }
}

#[allow(unreachable_code)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt: Opt = Opt::parse();

    // tracing setup
    let env_filter = if std::env::var(EnvFilter::DEFAULT_ENV)
        .unwrap_or_default()
        .is_empty()
    {
        EnvFilter::new("info")
    } else {
        EnvFilter::from_default_env()
    };
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(env_filter)
        .init();

    info!("Starting Akula ({})", version_string());

    let chains_config = akula::sentry::chain_config::ChainsConfig::new()?;
    let chain_config = chains_config.get(&opt.chain_name)?;

    // database setup
    let erigon_db = if let Some(erigon_data_dir) = opt.erigon_data_dir {
        let erigon_chain_data_dir = erigon_data_dir.join("chaindata");
        let erigon_db = akula::kv::mdbx::Environment::<mdbx::NoWriteMap>::open_ro(
            mdbx::Environment::new(),
            &erigon_chain_data_dir,
            akula::kv::tables::CHAINDATA_TABLES.clone(),
        )?;
        Some(Arc::new(erigon_db))
    } else {
        None
    };

    std::fs::create_dir_all(&opt.data_dir.0)?;
    let akula_chain_data_dir = opt.data_dir.chain_data_dir();
    let etl_temp_path = opt.data_dir.etl_temp_dir();
    let _ = std::fs::remove_dir_all(&etl_temp_path);
    std::fs::create_dir_all(&etl_temp_path)?;
    let etl_temp_dir =
        Arc::new(tempfile::tempdir_in(&etl_temp_path).context("failed to create ETL temp dir")?);
    let db = akula::kv::new_database(&akula_chain_data_dir)?;
    async {
        let txn = db.begin_mutable().await?;
        if akula::genesis::initialize_genesis(
            &txn,
            &*etl_temp_dir,
            chain_config.chain_spec().clone(),
        )
        .await?
        {
            txn.commit().await?;
        }

        Ok::<_, anyhow::Error>(())
    }
    .instrument(span!(Level::INFO, "", " Genesis initialization "))
    .await?;

    // staged sync setup
    let mut staged_sync = stagedsync::StagedSync::new();
    // staged_sync.set_min_progress_to_commit_after_stage(2);
    if let Some(erigon_db) = erigon_db.clone() {
        staged_sync.push(ConvertHeaders {
            db: erigon_db,
            max_block: Some(opt.snapshot),
        });
    } else {
        bail!("Must specify --erigon-data-dir");
    }
    staged_sync.push(BlockHashes {
        temp_dir: etl_temp_dir,
    });
    if let Some(erigon_db) = erigon_db {
        staged_sync.push(ConvertBodies { db: erigon_db });
    } else {
        // also add body download stage here
    }
    staged_sync.push(CumulativeIndex);
    staged_sync.push(SenderRecovery {
        batch_size: opt.sender_recovery_batch_size.try_into().unwrap(),
    });

    info!("Running staged sync");
    staged_sync.run(&db).await?;

    let execution = Execution {
        batch_size: opt.execution_batch_size.saturating_mul(1_000_000_000_u64),
        history_batch_size: opt
            .execution_history_batch_size
            .saturating_mul(1_000_000_000_u64),
        exit_after_batch: false,
        batch_until: None,
        commit_every: None,
        prune_from: BlockNumber(1_000_000_000_u64), // Infinite (i.e. never save history)
    };

    let tx = db.begin().await?;
    let execution_progress = EXECUTION.get_progress(&tx).await?.unwrap_or(BlockNumber(0));
    drop(tx);
    if execution_progress > opt.snapshot {
        execution::reset_execution(&db, chain_config.chain_spec().clone()).await?;
    }

    let tx = db.begin().await?;
    let execution_progress = EXECUTION.get_progress(&tx).await?.unwrap_or(BlockNumber(0));
    drop(tx);
    if execution_progress < opt.snapshot {
        execution::run_execution(execution, &db, opt.snapshot).await?;
    }

    let output = match opt.output {
        Some(path) => path,
        None => std::env::current_dir()?.join(opt.snapshot.to_string()),
    };
    snapshot::snapshot(&db, &output).await?;

    Ok(())
}
