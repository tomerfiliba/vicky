use std::ops::Range;
use std::time::Duration;

use rand::Rng;
use vicky_store::{Config, Result, VickyStore};

const TARGET: u32 = 1_000_000;

fn child_inserts() -> Result<()> {
    // our job is to create 1M entries while being killed by our evil parent

    let store = VickyStore::open("dbdir", Config::default())?;
    let highest_bytes = store.get("highest")?.unwrap_or(vec![0, 0, 0, 0]);
    let highest = u32::from_le_bytes([
        highest_bytes[0],
        highest_bytes[1],
        highest_bytes[2],
        highest_bytes[3],
    ]);

    if highest == TARGET - 1 {
        println!("child finished (already at {highest})");
        return Ok(());
    }

    println!("child starting at {highest}");

    for i in highest..TARGET {
        store.set(&i.to_le_bytes(), "i am a key")?;
        store.set("highest", &i.to_le_bytes())?;
    }
    println!("child finished");

    Ok(())
}

fn child_removals() -> Result<()> {
    // our job is to remove 1M entries while being killed by our evil parent

    let store = VickyStore::open("dbdir", Config::default())?;
    let lowest_bytes = store.get("lowest")?.unwrap_or(vec![0, 0, 0, 0]);
    let lowest = u32::from_le_bytes([
        lowest_bytes[0],
        lowest_bytes[1],
        lowest_bytes[2],
        lowest_bytes[3],
    ]);

    if lowest == TARGET - 1 {
        println!("child finished (already at {lowest})");
        return Ok(());
    }

    println!("child starting at {lowest}");

    for i in lowest..TARGET {
        store.remove(&i.to_le_bytes())?;
        store.set("lowest", &i.to_le_bytes())?;
    }
    println!("child finished");

    Ok(())
}

fn child_collection_inserts() -> Result<()> {
    // our job is to insert 1M entries to a collection while being killed by our evil parent

    let store = VickyStore::open("dbdir", Config::default())?;

    let highest_bytes = store.get("coll_highest")?.unwrap_or(vec![0, 0, 0, 0]);
    let highest = u32::from_le_bytes([
        highest_bytes[0],
        highest_bytes[1],
        highest_bytes[2],
        highest_bytes[3],
    ]);

    if highest == TARGET - 1 {
        println!("child finished (already at {highest})");
        return Ok(());
    }

    println!("child starting at {highest}");

    for i in highest..TARGET {
        store.set_in_collection("xxx", &i.to_le_bytes(), "yyy")?;
        store.set("coll_highest", &i.to_le_bytes())?;
    }
    println!("child finished");

    Ok(())
}

fn parent_run(mut child_func: impl FnMut() -> Result<()>, sleep: Range<u64>) -> Result<()> {
    for i in 0.. {
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0);
        if pid == 0 {
            let res = child_func();
            res.unwrap();
            unsafe { libc::exit(0) };
        } else {
            // parent
            std::thread::sleep(Duration::from_millis(
                rand::thread_rng().gen_range(sleep.clone()),
            ));
            let mut status = 0i32;
            let rc = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
            if rc == 0 {
                println!("[{i}] killing child");
                unsafe {
                    libc::kill(pid, libc::SIGKILL);
                    libc::wait(&mut status);
                };
            } else {
                assert!(rc > 0);
                if !libc::WIFSIGNALED(status) && libc::WEXITSTATUS(status) != 0 {
                    panic!("child crashed at iteration {i}")
                }

                println!("child finished in {i} iterations");
                break;
            }
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    _ = std::fs::remove_dir_all("dbdir");

    parent_run(child_inserts, 10..300)?;

    {
        println!("Parent starts validating the DB...");

        let store = VickyStore::open("dbdir", Config::default())?;
        assert_eq!(
            store.remove("highest")?,
            Some((TARGET - 1).to_le_bytes().to_vec())
        );
        let mut count = 0;
        for res in store.iter() {
            let (k, v) = res?;
            assert_eq!(v, b"i am a key");
            let k = u32::from_le_bytes([k[0], k[1], k[2], k[3]]);
            assert!(k < TARGET);
            count += 1;
        }
        assert_eq!(count, TARGET);

        println!("DB validated successfully");
    }

    parent_run(child_removals, 10..30)?;

    {
        println!("Parent starts validating the DB...");

        let store = VickyStore::open("dbdir", Config::default())?;
        assert_eq!(
            store.remove("lowest")?,
            Some((TARGET - 1).to_le_bytes().to_vec())
        );
        assert_eq!(store.iter().count(), 0);

        println!("DB validated successfully");
    }

    parent_run(child_collection_inserts, 10..30)?;

    {
        println!("Parent starts validating the DB...");

        let store = VickyStore::open("dbdir", Config::default())?;
        assert_eq!(
            store.remove("coll_highest")?,
            Some((TARGET - 1).to_le_bytes().to_vec())
        );

        for (i, res) in store.iter_collection("xxx").enumerate() {
            let (k, v) = res?.unwrap();
            assert_eq!(k, (i as u32).to_le_bytes());
            assert_eq!(v, b"yyy");
        }

        println!("DB validated successfully");
    }

    Ok(())
}
