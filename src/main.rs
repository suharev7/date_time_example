use chrono::offset::TimeZone;
use clickhouse_rs::{row, Block, Pool};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let ddl = r"
        CREATE TABLE default(
            `id` UInt32,
            `date` DateTime64(9, 'UTC')
        )
        ENGINE = MergeTree()
        PRIMARY KEY id";

    let url = "tcp://localhost:9000?compression=lz4&ping_timeout=2s&retry_timeout=3s";
    let pool = Pool::new(url);

    let mut client = pool.get_handle().await?;
    client.execute(ddl).await?;

    let date = chrono_tz::UTC
        .ymd(2020, 2, 3)
        .and_hms_nano(13, 45, 50, 8927265);

    let mut b = Block::new();
    b.push(
        row! { // notice the use of row macro for adding elements to a block
            id: 1u32,
            date,
        },
    )?;
    dbg!(b.get_column("date").unwrap().sql_type()); // prints DateTime(DateTime32)
    client.insert("default", b).await?;

    Ok(())
}
