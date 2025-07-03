use std::{
    io::{Error, Read},
    os::unix::process::ExitStatusExt,
    process::Command,
    process::Stdio,
};

#[test]
fn test_arguments_all() -> Result<(), Error> {
    let mut pgcache = Command::new(env!("CARGO_BIN_EXE_pgcache"))
        .arg("--origin_host")
        .arg("127.0.0.1")
        .arg("--origin_port")
        .arg("4444")
        .arg("--origin_user")
        .arg("test_user")
        .arg("--origin_database")
        .arg("test_db")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("run pgcache");

    //wait to read some data to make sure pgcache process is initialized before proceeding
    let mut buf = [0u8; 256];
    let _ = pgcache.stdout.take().unwrap().read(&mut buf);

    pgcache.kill().expect("command killed");
    let status = pgcache.wait().expect("exit_status");
    assert_eq!(status.signal(), Some(9)); //9 is the status from being killed
    Ok(())
}

#[test]
fn test_arguments_missing_origin_host() -> Result<(), Error> {
    let mut pgcache = Command::new(env!("CARGO_BIN_EXE_pgcache"))
        .arg("--origin_port")
        .arg("4444")
        .arg("--origin_user")
        .arg("test_user")
        .arg("--origin_database")
        .arg("test_db")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("run pgcache");

    //wait to read some data to make sure pgcache process is initialized before proceeding
    let mut buf = [0u8; 256];
    let _ = pgcache.stdout.take().unwrap().read(&mut buf);

    // pgcache.kill().expect("command killed");
    let status = pgcache.wait().expect("exit_status");
    assert_eq!(status.code(), Some(1));
    Ok(())
}

#[test]
fn test_arguments_missing_origin_port() -> Result<(), Error> {
    let mut pgcache = Command::new(env!("CARGO_BIN_EXE_pgcache"))
        .arg("--origin_host")
        .arg("127.0.0.1")
        .arg("--origin_user")
        .arg("test_user")
        .arg("--origin_database")
        .arg("test_db")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("run pgcache");

    //wait to read some data to make sure pgcache process is initialized before proceeding
    let mut buf = [0u8; 256];
    let _ = pgcache.stdout.take().unwrap().read(&mut buf);

    // pgcache.kill().expect("command killed");
    let status = pgcache.wait().expect("exit_status");
    assert_eq!(status.code(), Some(1));
    Ok(())
}

#[test]
fn test_arguments_missing_origin_user() -> Result<(), Error> {
    let mut pgcache = Command::new(env!("CARGO_BIN_EXE_pgcache"))
        .arg("--origin_host")
        .arg("127.0.0.1")
        .arg("--origin_port")
        .arg("4444")
        .arg("--origin_database")
        .arg("test_db")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("run pgcache");

    //wait to read some data to make sure pgcache process is initialized before proceeding
    let mut buf = [0u8; 256];
    let _ = pgcache.stdout.take().unwrap().read(&mut buf);

    // pgcache.kill().expect("command killed");
    let status = pgcache.wait().expect("exit_status");
    assert_eq!(status.code(), Some(1));
    Ok(())
}

#[test]
fn test_arguments_missing_origin_database() -> Result<(), Error> {
    let mut pgcache = Command::new(env!("CARGO_BIN_EXE_pgcache"))
        .arg("--origin_host")
        .arg("127.0.0.1")
        .arg("--origin_port")
        .arg("4444")
        .arg("--origin_user")
        .arg("test_user")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("run pgcache");

    //wait to read some data to make sure pgcache process is initialized before proceeding
    let mut buf = [0u8; 256];
    let _ = pgcache.stdout.take().unwrap().read(&mut buf);

    // pgcache.kill().expect("command killed");
    let status = pgcache.wait().expect("exit_status");
    assert_eq!(status.code(), Some(1));
    Ok(())
}
