mod common;

use vicky_store::{Config, Result, VickyStore};

use crate::common::run_in_tempdir;

#[test]
fn test_collections() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = VickyStore::open(dir, Config::default())?;

        db.add_to_collection("class1", "john", "100")?;
        db.add_to_collection("class1", "greg", "98")?;
        db.add_to_collection("class2", "john", "72")?;
        db.add_to_collection("class2", "boris", "89")?;

        assert_eq!(db.get_from_collection("class1", "greg")?, Some("98".into()));

        for (k, v) in db.iter_collection("class1")? {
            println!("class1: {:?} => {:?}", k, v);
        }

        for (k, v) in db.iter_collection("class2")? {
            println!("class2: {:?} => {:?}", k, v);
        }

        Ok(())
    })
}
