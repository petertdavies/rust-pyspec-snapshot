use akula::kv::tables::*;
use akula::kv::traits::{walk, MutableKV, Transaction};
use akula::stagedsync::stages::EXECUTION;
use ethereum_types::H256;
use sha3::{Digest, Keccak256};
use tokio::pin;
use tokio_stream::StreamExt;
use tracing::*;

pub async fn snapshot<DB: MutableKV>(db: &DB, path: &std::path::Path) -> anyhow::Result<()> {
    let tx = db.begin().await?;
    let block_number = EXECUTION.get_progress(&tx).await?.unwrap_or_default();
    let pyspec_db = ethereum_pyspec_db::DB::open(&path)?;
    let mut pyspec_tx = pyspec_db.begin_mutable()?;

    pyspec_tx.set_metadata(b"block_number", block_number.to_string().as_bytes())?;

    let mut cursor = tx.cursor(Account).await?;
    let stream = walk(&mut cursor, None);
    pin!(stream);

    let empty_code_hash = H256::from_slice(&Keccak256::digest(&[]));

    while let Some(res) = stream.next().await {
        let (address, account) = res?;
        let code = if account.code_hash == empty_code_hash {
            Vec::new()
        } else {
            tx.get(Code, account.code_hash).await?.unwrap().to_vec()
        };
        let pyspec_account = ethereum_pyspec_db::Account {
            nonce: account.nonce,
            balance: account.balance,
            code,
        };
        pyspec_tx.set_account(address, Some(pyspec_account));
    }

    let mut cursor = tx.cursor(Storage).await?;
    let stream = walk(&mut cursor, None);
    pin!(stream);
    while let Some(res) = stream.next().await {
        let (address, (key, value)) = res?;
        pyspec_tx.set_storage(address, key, value)?;
    }

    info!("State root is {:?}", pyspec_tx.state_root()?);
    pyspec_tx.commit()?;

    Ok(())
}
