extern crate getopts;
extern crate dotenv;
extern crate serde;
extern crate mysql_async;

use std::error::Error;
use deadpool_postgres::{ManagerConfig, RecyclingMethod, Runtime};
use deadpool_postgres::tokio_postgres::{NoTls, config};
use workerpool::Pool;
use workerpool::thunk::{ThunkWorker, Thunk};
use mysql_async::{Pool as MysqlPool};
use mysql_async::prelude::Queryable;
use futures::executor;
use dotenv::dotenv;
use tokio::task::JoinHandle;

struct WorkOrder(i64, i64, i32);
fn print_usage(program: &str, opts: getopts::Options) {
    let brief = format!("Usage: {} [options]", program);
    print!("{}", opts.usage(&brief))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    let args: Vec<String> = std::env::args().collect();
    let program = args[0].clone();
    let mut opts = getopts::Options::new();


    opts.optopt("w", "workers", "Number of workers", "WORKERS");
    opts.optopt("s", "start", "Start ID", "STARTID");
    opts.optopt("f", "finish", "Stop/finish ID", "STOPID");
    opts.optopt("", "source", "Source MySQL DSN see https://docs.rs/mysql/22.1.0/mysql/", "DSN");
    opts.optopt("", "dest", "Dest Pg DSN see https://docs.rs/tokio-postgres/0.7.6/tokio_postgres/config/struct.Config.html", "DSN");
    opts.optflag("h", "help", "print this help");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => { m }
        Err(f) => { panic!("{}", f.to_string())}
    };

    if matches.opt_present("h") {
        print_usage(&program, opts);
        return Ok(());
    }

    let n_workers = matches.opt_get_default("w", 32i32).unwrap();

    /*
    loop {
        match opts.next().transpose()? {
            None => break,
            Some(opt) => match opt {
                getopt::Opt('w', Some(string)) => n_workers = string.parse::<usize>().unwrap(),
                getopt::Opt('s', Some(string)) => start_id = string.parse::<i64>().unwrap(),
                getopt::Opt('f', Some(string)) => stop_id = string.parse::<i64>().unwrap(),
                _ => unreachable!(),
            }
        }
    }

     */

    let worker_pool = Pool::<ThunkWorker<WorkOrder>>::new(n_workers.try_into().unwrap());
    //let pool = Pool::<WorkOrderProcess>::with_name("mig_worker".into(),n_workers);

    let src_dsn = matches.opt_str("source").unwrap_or_default();

    let source_pool = MysqlPool::new(&*src_dsn);

    // bloody...i guess deadpool doesn't implement/expose/have from_str that tokio-postgres does?
    let mut dest_cfg = deadpool_postgres::Config::new();
    dest_cfg.dbname = matches.opt_str("ddb");
    dest_cfg.host = matches.opt_str("dhost");
    // deconstruct the port...
    let tmt = matches.opt_str("dport");
    dest_cfg.port = match tmt {
        Some(portstr) => Some(portstr.parse::<u16>().unwrap()),
        None => None
    };
    dest_cfg.manager = Some(ManagerConfig{ recycling_method: RecyclingMethod::Fast });

    let dest_pool = dest_cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();

    // remember our spawns so we can wait on them...
    let mut handles = vec![];
    // push work, checking for done results each loop...
    for _i in 0..1024 {
        let nwo = WorkOrder(64,64, 0);
        let thread_source_pool = source_pool.clone();
        let thread_dest_pool = dest_pool.clone();

        // the closure is super extra sensitive to return typing because of the (discarded) join_all
        // back on the main thread, so we can't use await? anywhere in it basically.
        let handle: JoinHandle<Result<(), String>> = tokio::spawn(async move {
            let mut source_conn = thread_source_pool.get_conn().await.unwrap();
            source_conn.query_drop(r"SELECT * ").await.unwrap();
            std::mem::drop(source_conn);
            let mut dest_conn = thread_dest_pool.get().await.unwrap();
            dest_conn.prepare_cached("INSERT 101").await.unwrap();
            Ok(())
        }
        );
        handles.push(handle);
    }

    // now wait until they're all done

    Ok(())
}
