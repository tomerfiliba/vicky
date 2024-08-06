use std::ops::Bound;

use simd_itertools::PositionSimd;

use crate::{
    hashing::{PartedHash, INVALID_SIG},
    shard::{InsertMode, KVPair, Shard, ShardRow},
    store::{COLLITEM_NAMESPACE, COLL_NAMESPACE},
    Result, VickyStore,
};

/*struct CollIterator<'a> {
    store: &'a VickyStore,
    chunk_index: usize,
    item_key_suffix: [u8; PartedHash::LEN + COLL_NAMESPACE.len()],
    coll_key: Vec<u8>,

}*/

impl VickyStore {
    fn make_coll_key(&self, coll_key: &[u8]) -> (PartedHash, Vec<u8>) {
        let mut full_key = coll_key.to_owned();
        full_key.extend_from_slice(COLL_NAMESPACE);
        (PartedHash::new(&self.config.hash_seed, &full_key), full_key)
    }

    fn make_item_key(&self, coll_ph: PartedHash, item_key: &[u8]) -> (PartedHash, Vec<u8>) {
        let mut full_key = item_key.to_owned();
        full_key.extend_from_slice(&coll_ph.to_bytes());
        full_key.extend_from_slice(COLLITEM_NAMESPACE);
        (PartedHash::new(&self.config.hash_seed, &full_key), full_key)
    }

    pub(crate) fn operate_on_key<T>(
        &self,
        key: &[u8],
        func: impl FnOnce(
            &Shard,
            &mut ShardRow,
            PartedHash,
            (usize, usize, u64),
            Option<KVPair>,
        ) -> Result<T>,
    ) -> Result<T> {
        let ph = PartedHash::new(&self.config.hash_seed, key);
        self.shards
            .read()
            .unwrap()
            .lower_bound(Bound::Excluded(&(ph.shard_selector as u32)))
            .peek_next()
            .unwrap()
            .1
            .operate_on_key(ph, key, func)
    }

    pub fn add_to_collection<
        B1: AsRef<[u8]> + ?Sized,
        B2: AsRef<[u8]> + ?Sized,
        B3: AsRef<[u8]> + ?Sized,
    >(
        &self,
        coll_key: &B1,
        item_key: &B2,
        val: &B3,
    ) -> Result<()> {
        let coll_key = coll_key.as_ref();
        let item_key = item_key.as_ref();
        let val = val.as_ref();
        let (coll_ph, mut full_coll_key) = self.make_coll_key(coll_key);
        let (item_ph, full_item_key) = self.make_item_key(coll_ph, item_key);

        self.set_raw(&full_item_key, val)?;

        full_coll_key.extend_from_slice(&[0u8; size_of::<u64>()]);
        let key_len = full_coll_key.len();

        for block_idx in 0u64.. {
            full_coll_key[key_len - size_of::<u64>()..].copy_from_slice(&block_idx.to_le_bytes());

            let succ = self.operate_on_key(
                &full_coll_key,
                |shard, row, ph, (klen, vlen, offset), kvpair| {
                    if let Some((_, v)) = kvpair {
                        let entries_as_u64: &[u64] = unsafe { std::mem::transmute(&v[..]) };
                        if let Some(idx) = entries_as_u64.iter().position_simd(0) {
                            let byteoff = idx * size_of::<u64>();
                            assert!(byteoff <= vlen);
                            shard.write_raw(
                                &item_ph.to_bytes(),
                                offset + klen as u64 + byteoff as u64,
                            )?;
                            Ok(true)
                        } else {
                            Ok(false)
                        }
                    } else {
                        let mut buf = vec![0u8; 2048];
                        buf[0..size_of::<u64>()].copy_from_slice(&item_ph.to_bytes());
                        shard.insert_unlocked(
                            row,
                            ph,
                            &full_coll_key,
                            &buf,
                            InsertMode::GetOrCreate,
                        )?;
                        Ok(true)
                    }
                },
            )?;
            if succ {
                break;
            }
        }

        Ok(())
    }

    pub fn get_from_collection<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        coll_key: &B1,
        item_key: &B2,
    ) -> Result<Option<Vec<u8>>> {
        let coll_key = coll_key.as_ref();
        let item_key = item_key.as_ref();
        let (coll_ph, _) = self.make_coll_key(coll_key);
        let (_, full_item_key) = self.make_item_key(coll_ph, item_key);
        self.get_raw(&full_item_key)
    }

    pub fn iter_collection<B: AsRef<[u8]> + ?Sized>(&self, coll_key: &B) -> Result<Vec<KVPair>> {
        let coll_key = coll_key.as_ref();
        let (coll_ph, mut full_coll_key) = self.make_coll_key(coll_key);
        let mut suffix = [0u8; PartedHash::LEN + COLLITEM_NAMESPACE.len()];
        suffix[..PartedHash::LEN].copy_from_slice(&coll_ph.to_bytes());
        suffix[PartedHash::LEN..].copy_from_slice(COLLITEM_NAMESPACE);

        let mut pairs = vec![];

        full_coll_key.extend_from_slice(&[0u8; size_of::<u64>()]);
        let key_len = full_coll_key.len();

        for block_idx in 0u64.. {
            full_coll_key[key_len - size_of::<u64>()..].copy_from_slice(&block_idx.to_le_bytes());

            let Some(entries_buf) = self.get_raw(&full_coll_key)? else {
                break;
            };

            let entries_as_u64: &[u64] = unsafe { std::mem::transmute(&entries_buf[..]) };
            for &ph_u64 in entries_as_u64 {
                let ph = PartedHash::from_u64(ph_u64);
                if ph.signature == INVALID_SIG {
                    continue;
                }
                for res in self.get_by_hash(ph) {
                    let (mut k, v) = res?;
                    if k.ends_with(&suffix) {
                        k.truncate(k.len() - suffix.len());
                        pairs.push((k, v));
                    }
                }
            }
        }

        Ok(pairs)
    }
}
