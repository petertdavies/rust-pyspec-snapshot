use akula::kv::tables::*;
use akula::kv::traits::{MutableKV, MutableTransaction};
use akula::models::*;
use akula::stagedsync::stage::{ExecOutput, Stage, StageInput};
use akula::stagedsync::stages::{StageId, EXECUTION};
use akula::stages::Execution;
use akula::state::*;
use std::time::Instant;
use tracing::*;

pub async fn reset_execution<'db, DB: MutableKV>(
    db: &'db DB,
    chainspec: ChainSpec,
) -> anyhow::Result<()> {
    let tx = db.begin_mutable().await?;

    info!("clearing Account");
    tx.clear_table(Account).await?;
    info!("clearing Storage");
    tx.clear_table(Storage).await?;
    info!("clearing AccountChangeSet");
    tx.clear_table(AccountChangeSet).await?;
    info!("clearing StorageChangeSet");
    tx.clear_table(StorageChangeSet).await?;
    info!("clearing AccountHistory");
    tx.clear_table(AccountHistory).await?;
    info!("clearing StorageHistory");
    tx.clear_table(StorageHistory).await?;
    info!("clearing Code");
    tx.clear_table(Code).await?;

    let genesis = chainspec.genesis.number;
    let mut state_buffer = Buffer::new(&tx, genesis, None);
    state_buffer.begin_block(genesis);
    // Allocate accounts
    if let Some(balances) = chainspec.balances.get(&genesis) {
        for (&address, &balance) in balances {
            state_buffer.update_account(
                address,
                None,
                Some(akula::models::Account {
                    balance,
                    ..Default::default()
                }),
            );
        }
    }
    state_buffer.write_to_db().await?;

    EXECUTION.save_progress(&tx, BlockNumber(0)).await?;
    tx.commit().await?;
    info!("Execution reset");
    Ok(())
}

pub async fn run_execution<'db, DB: MutableKV>(
    execution: Execution,
    db: &'db DB,
    to: BlockNumber,
) -> anyhow::Result<()> {
    let mut tx = db.begin_mutable().await?;
    let start_time = Instant::now();
    let senders_progress = StageId("SenderRecovery")
        .get_progress(&tx)
        .await?
        .unwrap_or_default();
    let start_progress = EXECUTION.get_progress(&tx).await?;
    let mut progress = start_progress;

    info!(
        "RUNNING from {} to {}",
        progress
            .map(|s| s.to_string())
            .unwrap_or_else(|| "genesis".to_string()),
        to.to_string()
    );

    let mut restarted = false;
    let final_progress = loop {
        let output = execution
            .execute(
                &mut tx,
                StageInput {
                    restarted,
                    first_started_at: (start_time, start_progress),
                    previous_stage: Some((StageId("SenderRecovery"), to)),
                    stage_progress: progress,
                },
            )
            .await?;

        match output {
            ExecOutput::Progress {
                done,
                stage_progress,
                must_commit,
                ..
            } => {
                EXECUTION.save_progress(&tx, stage_progress).await?;
                progress = Some(stage_progress);

                if done {
                    info!("Progress: {} To: {}", senders_progress, to);
                    // Break out and move to the next stage.
                    break stage_progress;
                }

                // Stage requested that we commit into database now.
                if must_commit {
                    // Commit and restart transaction.
                    debug!("Commit requested");
                    tx.commit().await?;
                    debug!("Commit complete");
                    tx = db.begin_mutable().await?;
                }

                restarted = true;
            }
            ExecOutput::Unwind { .. } => panic!("Unexpected Unwind!"),
        };
    };

    assert_eq!(final_progress, to);
    tx.commit().await?;
    Ok(())
}
