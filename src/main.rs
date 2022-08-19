#![feature(generators)]
#![feature(generator_trait)]

use std::fmt::Debug;
use std::ops::{Generator, GeneratorState};
use std::pin::Pin;
use std::time::Instant;

use akula::accessors::chain::{canonical_hash, header};
use akula::etl::collector::{Collector, CollectorIter};
use akula::kv::mdbx::MdbxTransaction;
use akula::kv::tables::{AccountChange, BitmapKey, StorageChange, StorageChangeKey, VariableVec};
use akula::{
    akula_tracing::{self, Component},
    binutil::AkulaDataDir,
    kv::{
        tables::{self, CHAINDATA_TABLES},
        traits::*,
    },
    models::*,
    stagedsync::format_duration,
};
use clap::Parser;
use croaring::Treemap;
use directories::ProjectDirs;
use mdbx::{NoWriteMap, RO};
use tracing::*;
use tracing_subscriber::prelude::*;

struct BlockNumbered<T> {
    block_number: BlockNumber,
    inner: T,
}

impl<T: TableEncode> TableEncode for BlockNumbered<T> {
    type Encoded = VariableVec<60>;

    fn encode(self) -> Self::Encoded {
        let mut v = Self::Encoded::default();
        v.try_extend_from_slice(&self.block_number.encode())
            .unwrap();
        v.try_extend_from_slice(self.inner.encode().as_ref())
            .unwrap();
        v
    }
}

impl<T: TableDecode> TableDecode for BlockNumbered<T> {
    fn decode(v: &[u8]) -> anyhow::Result<Self> {
        Ok(BlockNumbered {
            block_number: BlockNumber::decode(&v[..8]).unwrap(),
            inner: T::decode(&v[8..]).unwrap(),
        })
    }
}

fn open_db(
    data_dir: AkulaDataDir,
) -> anyhow::Result<akula::kv::mdbx::MdbxEnvironment<mdbx::NoWriteMap>> {
    akula::kv::mdbx::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &data_dir.chain_data_dir(),
        &CHAINDATA_TABLES,
    )
}

fn extract_indexes<Key, EncodedKey, DataTable>(
    tx: &MdbxTransaction<RO, NoWriteMap>,
    phase_number: usize,
    target_block: BlockNumber,
    table: DataTable,
    changes_collector: &mut Collector<BlockNumbered<Key>, ()>,
    head_collector: &mut Collector<Key, ()>,
) -> anyhow::Result<usize>
where
    Key: TableDecode + TableEncode<Encoded = EncodedKey> + Debug + Clone,
    EncodedKey: Ord,
    Vec<u8>: From<EncodedKey>,
    DataTable: Table<Key = BitmapKey<Key>, Value = Treemap, SeekKey = BitmapKey<Key>>,
    BitmapKey<Key>: TableDecode,
{
    let mut indexed = 0_usize;
    let mut total_indexed = 0_usize;
    let mut cur = tx.cursor(table)?;
    let (mut cur_key, mut cur_value) = cur.first()?.unwrap();
    loop {
        if cur_value.minimum().unwrap() <= target_block.0 {
            if cur_key.block_number < target_block {
                (cur_key, cur_value) = cur
                    .seek(BitmapKey {
                        inner: cur_key.inner,
                        block_number: target_block,
                    })?
                    .unwrap();
            };
            match cur_value
                .iter()
                .find(|&change_block| *target_block < change_block)
                .map(BlockNumber)
            {
                None => head_collector.push(cur_key.inner.clone(), ()),
                Some(block) => changes_collector.push(
                    BlockNumbered {
                        block_number: block,
                        inner: cur_key.inner.clone(),
                    },
                    (),
                ),
            }
            indexed += 1;
        }
        cur.seek(BitmapKey {
            inner: cur_key.inner,
            block_number: BlockNumber(u64::MAX),
        })?;
        (cur_key, cur_value) = match cur.next()? {
            None => break,
            Some(x) => x,
        };

        total_indexed += 1;
        if total_indexed % 1000000 == 0 {
            info!(
                "Phase {}: Extracted {} indexes, next item is {:?}",
                phase_number, total_indexed, cur_key.inner
            );
        }
    }

    Ok(indexed)
}

struct AnyhowIter<'a, T> {
    done: bool,
    inner: Pin<Box<dyn Generator<Yield = anyhow::Result<T>, Return = anyhow::Result<()>> + 'a>>,
}

impl<'a, T> Iterator for AnyhowIter<'a, T> {
    type Item = anyhow::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        match Pin::new(&mut self.inner).resume(()) {
            GeneratorState::Yielded(res) => Some(res),
            GeneratorState::Complete(res) => {
                self.done = true;
                match res {
                    Ok(()) => None,
                    Err(e) => Some(Err(e)),
                }
            }
        }
    }
}

fn extract_accounts_changes<'a>(
    tx: &'a MdbxTransaction<'a, RO, NoWriteMap>,
    collector_iter: CollectorIter<'a>,
) -> AnyhowIter<'a, AccountChange> {
    AnyhowIter {
        done: false,
        inner: Box::pin(move || {
            let mut cur = tx.cursor(tables::AccountChangeSet)?;
            for item in collector_iter {
                let BlockNumbered {
                    block_number: block,
                    inner: key,
                } = TableDecode::decode(&item?.0)?;
                anyhow::ensure!(block != BlockNumber(u64::MAX));
                yield Ok(cur.seek_both_range(block, key)?.unwrap());
            }
            Ok(())
        }),
    }
}

fn extract_accounts_head<'a>(
    tx: &'a MdbxTransaction<'a, RO, NoWriteMap>,
    collector_iter: CollectorIter<'a>,
) -> AnyhowIter<'a, (Address, Account)> {
    AnyhowIter {
        done: false,
        inner: Box::pin(move || {
            let mut cur = tx.cursor(tables::Account)?;
            for item in collector_iter {
                let key = TableDecode::decode(&item?.0)?;
                match cur.seek_exact(key)? {
                    None => (),
                    Some(x) => yield Ok(x),
                }
            }
            Ok(())
        }),
    }
}

fn extract_storage_head<'a>(
    tx: &'a MdbxTransaction<'a, RO, NoWriteMap>,
    collector_iter: CollectorIter<'a>,
) -> AnyhowIter<'a, (Address, (H256, U256))> {
    AnyhowIter {
        done: false,
        inner: Box::pin(move || {
            let mut cur = tx.cursor(tables::Storage)?;
            for item in collector_iter {
                let (address, location) = TableDecode::decode(&item?.0)?;
                match cur.seek_both_range(address, location)? {
                    None => (),
                    Some((location_, value)) => {
                        if location == location_ {
                            yield Ok((address, (location_, value)))
                        }
                    }
                }
            }
            Ok(())
        }),
    }
}

fn extract_storage_changes<'a>(
    tx: &'a MdbxTransaction<'a, RO, NoWriteMap>,
    collector_iter: CollectorIter<'a>,
) -> AnyhowIter<'a, (Address, StorageChange)> {
    AnyhowIter {
        done: false,
        inner: Box::pin(move || {
            let mut cur = tx.cursor(tables::StorageChangeSet)?;
            for item in collector_iter {
                let BlockNumbered {
                    block_number: block,
                    inner: (address, location),
                } = TableDecode::decode(&item?.0)?;
                anyhow::ensure!(block != BlockNumber(u64::MAX));
                let change = cur
                    .seek_both_range(
                        StorageChangeKey {
                            block_number: block,
                            address,
                        },
                        location,
                    )?
                    .unwrap();
                anyhow::ensure!(location == change.location);
                if change.value != U256::ZERO {
                    yield Ok((address, change));
                }
            }
            Ok(())
        }),
    }
}

#[derive(clap::Parser, Debug)]
struct Opts {
    #[clap(
        long = "akula-datadir",
        help = "Akula database directory path",
        default_value_t
    )]
    akula_data_dir: AkulaDataDir,

    #[clap(short = 'o', help = "Path to output pyspec db")]
    output_path: std::path::PathBuf,

    #[clap(short = 'b', help = "Block to extract database at")]
    target_block: BlockNumber,
}

fn main_() -> anyhow::Result<()> {
    let opts = Opts::parse();

    let cache_dir = ProjectDirs::from("", "", "akula-extract")
        .unwrap()
        .cache_dir()
        .to_path_buf();
    std::fs::create_dir_all(&cache_dir)?;
    let temp_dir = tempfile::tempdir_in(
        ProjectDirs::from("", "", "akula-extract")
            .unwrap()
            .cache_dir(),
    )?;
    let db_path = &opts.output_path;

    ethereum_pyspec_db::Db::delete(db_path)?;
    let mut pyspec_db = ethereum_pyspec_db::Db::file(db_path)?;
    let mut pyspec_tx = pyspec_db.begin_mut()?;

    let env = open_db(opts.akula_data_dir)?;
    let tx = env.begin()?;
    let mut written_items = 0_usize;

    info!("Phase 1: Extract account indexes");
    let start = Instant::now();

    let mut accounts_changes: Collector<BlockNumbered<Address>, ()> =
        Collector::new(&temp_dir, 200_000_000);
    let mut accounts_head: Collector<Address, ()> = Collector::new(&temp_dir, 200_000_000);

    let indexed = extract_indexes(
        &tx,
        1,
        opts.target_block,
        tables::AccountHistory,
        &mut accounts_changes,
        &mut accounts_head,
    )?;

    info!(
        "Phase 1: complete, indexed {}, took {}",
        indexed,
        format_duration(Instant::now() - start, true)
    );

    info!("Phase 2: Extract accounts");
    let mut code_hashes: Collector<H256, ()> = Collector::new(&temp_dir, 200_000_000);

    for (i, change) in extract_accounts_changes(&tx, accounts_changes.iter()).enumerate() {
        let change = change?;
        if let Some(account) = change.account {
            let pyspec_account = ethereum_pyspec_db::Account {
                nonce: account.nonce,
                balance: account.balance.to_be_bytes().into(),
                code_hash: account.code_hash.as_fixed_bytes().into(),
            };
            if account.code_hash != EMPTY_HASH {
                code_hashes.push(account.code_hash, ());
            }
            pyspec_tx.set_account(change.address.as_fixed_bytes().into(), Some(pyspec_account));
            written_items += 1;
        }

        if i != 0 && i % 1000000 == 0 {
            info!("Phase 2: Extracted {} accounts", i);
        }
        if written_items % 1000000 == 0 {
            let start = Instant::now();
            pyspec_tx.state_root()?;
            info!(
                "Flushed accounts to db, took {}",
                format_duration(Instant::now() - start, true)
            );
        }
    }
    drop(accounts_changes);

    info!("Phase 3: Extract accounts");

    for (i, item) in extract_accounts_head(&tx, accounts_head.iter()).enumerate() {
        let (address, account) = item?;
        pyspec_tx.set_account(
            address.to_fixed_bytes().into(),
            Some(ethereum_pyspec_db::Account {
                nonce: account.nonce,
                balance: account.balance.to_be_bytes().into(),
                code_hash: account.code_hash.as_fixed_bytes().into(),
            }),
        );
        if account.code_hash != EMPTY_HASH {
            code_hashes.push(account.code_hash, ());
        }
        written_items += 1;
        if written_items % 1000000 == 0 {
            let start = Instant::now();
            pyspec_tx.state_root()?;
            info!(
                "Flushed accounts to db, took {}",
                format_duration(Instant::now() - start, true)
            );
        }
        if i != 0 && i % 1000000 == 0 {
            info!(
                "Phase 3: Extracted {} accounts, current address is {}",
                i, address
            );
        }
    }
    drop(accounts_head);

    info!("Phase 4: Extract code");

    let mut cur = tx.cursor(tables::Code)?;
    let mut last_hash = EMPTY_HASH;
    for (i, item) in code_hashes.iter().enumerate() {
        let code_hash: H256 = TableDecode::decode(&item?.0)?;
        if code_hash != last_hash {
            let (_, code) = cur.seek_exact(code_hash)?.unwrap();
            pyspec_tx.store_code(&code)?;
            last_hash = code_hash;
        }
        if i != 0 && i % 100000 == 0 {
            info!(
                "Phase 4: Extracted {} codes, current code hash is {}",
                i, code_hash
            );
        }
    }
    drop(code_hashes);

    info!("Phase 5: Extract storage indexes");

    let mut storage_changes: Collector<BlockNumbered<(Address, H256)>, ()> =
        Collector::new(&temp_dir, 200_000_000);
    let mut storage_head: Collector<(Address, H256), ()> = Collector::new(&temp_dir, 200_000_000);
    let indexed = extract_indexes(
        &tx,
        5,
        opts.target_block,
        tables::StorageHistory,
        &mut storage_changes,
        &mut storage_head,
    )?;

    info!("Phase 5: Complete, extracted {} indexes", indexed);

    info!("Phase 6: Extract storage");

    for (i, item) in extract_storage_changes(&tx, storage_changes.iter()).enumerate() {
        let (address, change) = item?;
        let value: ethereum_types::U256 = change.value.to_be_bytes().into();
        if !value.is_zero() {
            pyspec_tx.set_storage(
                address.as_fixed_bytes().into(),
                change.location.as_fixed_bytes().into(),
                value,
            )?;
            written_items += 1;
        }
        if i != 0 && i % 1000000 == 0 {
            info!(
                "Phase 6: Extracted {} storage keys, current address is {}, current key is {}",
                i, address, change.location
            );
        }
        if written_items % 1000000 == 0 {
            let start = Instant::now();
            pyspec_tx.state_root()?;
            info!(
                "Flushed accounts to db, took {}",
                format_duration(Instant::now() - start, true)
            );
        }
    }
    drop(storage_changes);

    info!("Phase 7: Extract storage");

    for (i, item) in extract_storage_head(&tx, storage_head.iter()).enumerate() {
        let (address, (location, value)) = item?;
        pyspec_tx.set_storage(
            address.as_fixed_bytes().into(),
            location.as_fixed_bytes().into(),
            value.to_be_bytes().into(),
        )?;
        written_items += 1;

        if i != 0 && i % 1000000 == 0 {
            info!(
                "Extracted {} storage keys, current address is {}, current key is {}",
                i, address, location
            );
        }
        if written_items % 1000000 == 0 {
            let start = Instant::now();
            pyspec_tx.state_root()?;
            info!(
                "Flushed accounts to db, took {}",
                format_duration(Instant::now() - start, true)
            );
        }
    }
    drop(storage_head);

    info!("Wrote {} items", written_items);

    let state_root = pyspec_tx.state_root()?;
    let state_root_from_header = header::read(
        &tx,
        canonical_hash::read(&tx, opts.target_block)?.unwrap(),
        opts.target_block,
    )?
    .unwrap()
    .state_root;
    anyhow::ensure!(state_root == state_root_from_header.to_fixed_bytes().into());
    pyspec_tx.set_metadata(
        "block_number".as_bytes(),
        opts.target_block.to_string().as_bytes(),
    )?;
    pyspec_tx.set_metadata("chain_id".as_bytes(), "1".as_bytes())?;
    pyspec_tx.commit()?;
    info!("Complete: State root is {:?}", state_root);
    Ok(())
}

fn main() {
    akula_tracing::build_subscriber(Component::Core).init();
    main_().unwrap();
}
