use anyhow::{Context, Result};
use clap::Parser;
use mysql_async::prelude::*;
use mysql_async::{Conn, Pool};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

/// HDFS log entry structure
#[derive(Debug, Serialize, Deserialize)]
struct LogEntry {
    timestamp: i64,
    severity_text: String,
    body: String,
    tenant_id: i32,
}

/// Command-line arguments
#[derive(Parser, Debug)]
#[command(name = "hdfs-log-reader")]
#[command(about = "Process HDFS logs and insert into database", long_about = None)]
struct Args {
    /// Name of the database table
    table_name: String,

    /// Maximum number of rows to process
    #[arg(long)]
    max_rows: Option<usize>,

    /// Number of rows to process in each batch
    #[arg(long, default_value_t = 50000)]
    batch_size: usize,

    /// TiDB address to connect to
    #[arg(long, default_value = "localhost")]
    tidb_host: String,

    /// TiDB port to connect to
    #[arg(long, default_value_t = 4000)]
    tidb_port: u16,

    /// Directory containing the asset files (when empty, uses current directory)
    #[arg(long, default_value = "")]
    asset_dir: String,
}

/// Connect to the database
async fn connect_to_database(tidb_host: &str, tidb_port: u16) -> Result<Conn> {
    println!("Connecting to tidb, host={} port={}", tidb_host, tidb_port);

    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(tidb_host)
        .tcp_port(tidb_port)
        .user(Some("root"))
        .pass(Some(""))
        .db_name(Some("test"));

    let pool = Pool::new(opts);
    let conn = pool
        .get_conn()
        .await
        .context("Error connecting to the database")?;

    println!("Successfully connected to the database");
    Ok(conn)
}

/// Create the HDFS log table
async fn create_hdfs_log_table(conn: &mut Conn, table_name: &str) -> Result<()> {
    // Drop table if exists
    conn.exec_drop(format!("DROP TABLE IF EXISTS {}", table_name), ())
        .await
        .context("Error dropping table")?;
    println!("Table {} dropped successfully", table_name);

    // Create table
    let create_table_sql = format!(
        r#"
        CREATE TABLE IF NOT EXISTS {} (
            id BIGINT AUTO_INCREMENT,
            timestamp BIGINT,
            severity_text VARCHAR(50),
            body TEXT,
            tenant_id INT,
            PRIMARY KEY (tenant_id, id)
        ) AUTO_INCREMENT = 1000
        "#,
        table_name
    );

    conn.exec_drop(create_table_sql, ())
        .await
        .context("Error creating table")?;
    println!("Table {} created successfully", table_name);

    Ok(())
}

/// Read HDFS logs from a JSON file
fn read_hdfs_logs(
    file_path: &str,
    max_rows: Option<usize>,
) -> Result<impl Iterator<Item = Result<LogEntry>>> {
    let file = File::open(file_path)
        .with_context(|| format!("Error opening file {}", file_path))?;
    let reader = BufReader::new(file);

    let iter = reader
        .lines()
        .enumerate()
        .take(max_rows.unwrap_or(usize::MAX))
        .map(|(i, line)| {
            let line = line.with_context(|| format!("Error reading line {}", i + 1))?;
            serde_json::from_str::<LogEntry>(&line)
                .with_context(|| format!("Error parsing JSON at line {}", i + 1))
        });

    Ok(iter)
}

/// Insert a batch of HDFS logs into the database
async fn insert_hdfs_logs_batch(
    conn: &mut Conn,
    table_name: &str,
    logs: &[LogEntry],
) -> Result<()> {
    let sql = format!(
        "INSERT INTO {} (timestamp, severity_text, body, tenant_id) VALUES (?, ?, ?, ?)",
        table_name
    );

    let params: Vec<_> = logs
        .iter()
        .map(|log| {
            (
                log.timestamp,
                &log.severity_text,
                &log.body,
                log.tenant_id,
            )
        })
        .collect();

    conn.exec_batch(sql, params)
        .await
        .context("Error inserting batch")?;

    println!(
        "{} logs from batch inserted successfully into {}",
        logs.len(),
        table_name
    );

    Ok(())
}

/// Process HDFS logs
async fn process_hdfs_logs(args: Args) -> Result<()> {
    let infilename = if args.asset_dir.is_empty() {
        PathBuf::from("hdfs-logs-multitenants.json")
    } else {
        PathBuf::from(&args.asset_dir).join("hdfs-logs-multitenants.json")
    };

    println!(
        "Processing logs from '{}' in batches of {}",
        infilename.display(), args.batch_size
    );

    let mut conn = connect_to_database(&args.tidb_host, args.tidb_port).await?;
    create_hdfs_log_table(&mut conn, &args.table_name).await?;

    let log_iter = read_hdfs_logs(
        infilename.to_str().context("Invalid file path")?,
        args.max_rows,
    )?;

    let mut batch = Vec::with_capacity(args.batch_size);
    let mut total_inserted = 0;
    let mut total_read = 0;

    for log_result in log_iter {
        match log_result {
            Ok(log) => {
                batch.push(log);
                total_read += 1;

                if batch.len() >= args.batch_size {
                    insert_hdfs_logs_batch(&mut conn, &args.table_name, &batch).await?;
                    total_inserted += batch.len();
                    println!("Total logs inserted so far: {}", total_inserted);
                    batch.clear();
                }
            }
            Err(e) => {
                eprintln!("Error processing log entry: {}", e);
            }
        }
    }

    // Process remaining batch
    if !batch.is_empty() {
        insert_hdfs_logs_batch(&mut conn, &args.table_name, &batch).await?;
        total_inserted += batch.len();
        println!("Total logs inserted so far: {}", total_inserted);
    }

    println!("Read {} total log entries from {}", total_read, infilename.display());

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!(
        "\nProcessing HDFS logs into database table '{}':",
        args.table_name
    );

    process_hdfs_logs(args).await?;

    println!("\nData processing complete.");

    Ok(())
}
