use akula::kv::tables::*;
use akula::kv::traits::{walk, MutableKV, Transaction};
use akula::stagedsync::stages::EXECUTION;
use ethereum_types::H256;
use sha3::{Digest, Keccak256};
use tokio::pin;
use tokio_stream::StreamExt;
use tracing::*;

pub async fn snapshot<DB: MutableKV>(db: &DB, path: &std::path::Path) -> anyhow::Result<()> {
    ethereum_pyspec_db::DB::delete_db(path)?;
    let tx = db.begin().await?;
    let block_number = EXECUTION.get_progress(&tx).await?.unwrap_or_default();
    let mut items_written = 0;
    let mut pyspec_db = ethereum_pyspec_db::DB::open(&path)?;
    {
        let mut pyspec_tx = pyspec_db.begin_mutable()?;
        pyspec_tx.set_metadata(b"block_number", block_number.to_string().as_bytes())?;
        pyspec_tx.commit()?;
    }

    let mut cursor = tx.cursor(Account).await?;
    let stream = walk(&mut cursor, None);
    pin!(stream);

    let empty_code_hash = H256::from_slice(&Keccak256::digest(&[]));

    let mut last_address;
    'outer: loop {
        let mut pyspec_tx = pyspec_db.begin_mutable()?;
        loop {
            let (address, account) = match stream.next().await {
                None => {
                    pyspec_tx.commit()?;
                    break 'outer;
                }
                Some(x) => x?,
            };
            if account.code_hash != empty_code_hash {
                pyspec_tx.store_code(&tx.get(Code, account.code_hash).await?.unwrap())?;
            }
            let pyspec_account = ethereum_pyspec_db::Account {
                nonce: account.nonce,
                balance: account.balance,
                code_hash: account.code_hash,
            };
            pyspec_tx.set_account(address, Some(pyspec_account));
            items_written += 1;
            last_address = address;
            if items_written % 1_000_000 == 0 {
                break;
            }
        }
        pyspec_tx.commit()?;
        info!("Wrote {} items, address={}", items_written, last_address);
    }

    let mut cursor = tx.cursor(Storage).await?;
    let stream = walk(&mut cursor, None);
    pin!(stream);
    let mut last_key;
    'outer2: loop {
        let mut pyspec_tx = pyspec_db.begin_mutable()?;
        loop {
            let (address, (key, value)) = match stream.next().await {
                None => {
                    pyspec_tx.commit()?;
                    break 'outer2;
                }
                Some(x) => x?,
            };
            pyspec_tx.set_storage(address, key, value)?;
            items_written += 1;
            last_address = address;
            last_key = key;
            if items_written % 1_000_000 == 0 {
                break;
            }
        }
        pyspec_tx.commit()?;
        info!(
            "Wrote {} items, address={}, storage={}",
            items_written, last_address, last_key
        );
    }

    {
        let mut pyspec_tx = pyspec_db.begin_mutable()?;
        info!("Complete, wrote {} items", items_written);
        info!("State root is {:?}", pyspec_tx.state_root()?);
        pyspec_tx.commit()?;
    }

    Ok(())
}
