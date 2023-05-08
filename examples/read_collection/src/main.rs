use polars::prelude::*;
use polars_mongo::prelude::*;

pub fn main() -> PolarsResult<()> {
    let connection_str = "mongodb://127.0.0.1:27017".into();
    let db = "stock_history_daily".into();
    let collection = "000001".into();

    let df = LazyFrame::scan_mongo_collection(MongoScanOptions {
        batch_size: None,
        connection_str,
        db,
        collection,
        infer_schema_length: Some(1000),
        n_rows: Some(129),
    })?
    .collect()?;

    dbg!(df);
    Ok(())
}
