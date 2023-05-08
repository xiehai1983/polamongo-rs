//! Polars mongo is a connector to read from a mongodb collection into a Polars dataframe.
//! Usage:
//! ```rust
//! use polars::prelude::*;
//! use polars_mongo::prelude::*;
//!
//! pub fn main() -> PolarsResult<()> {
//!     let connection_str = std::env::var("POLARS_MONGO_CONNECTION_URI").unwrap();
//!     let db = std::env::var("POLARS_MONGO_DB").unwrap();
//!     let collection = std::env::var("POLARS_MONGO_COLLECTION").unwrap();
//!
//!     let df = LazyFrame::scan_mongo_collection(MongoScanOptions {
//!         batch_size: None,
//!         connection_str,
//!         db,
//!         collection,
//!         infer_schema_length: Some(1000),
//!         n_rows: None,
//!     })?
//!     .collect()?;
//!
//!     dbg!(df);
//!     Ok(())
//! }
//!
#![deny(clippy::all)]
use mongodb::bson::doc;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
mod buffer;
mod conversion;
pub mod prelude;

use crate::buffer::*;

use conversion::Wrap;
use polars::export::rayon::prelude::*;
use polars::{frame::row::*, prelude::*};
use polars_core::POOL;

use mongodb::{
    bson::{Bson, Document},
    options::{ClientOptions, FindOptions},
    sync::{Client, Collection, Cursor},
};
use polars_core::utils::accumulate_dataframes_vertical;

pub struct MongoScan {
    client_options: ClientOptions,
    db: String,
    collection_name: String,
    pub collection: Option<Collection<Document>>,
    pub n_threads: Option<usize>,
    pub batch_size: Option<usize>,
    pub rechunk: bool,
}

impl MongoScan {
    pub fn with_rechunk(mut self, rechunk: bool) -> Self {
        self.rechunk = rechunk;
        self
    }
    pub fn with_batch_size(mut self, batch_size: Option<usize>) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn new(connection_str: String, db: String, collection: String) -> PolarsResult<Self> {
        let client_options = ClientOptions::parse(connection_str).map_err(|e| {
            PolarsError::InvalidOperation(format!("unable to connect to mongodb: {}", e).into())
        })?;

        Ok(MongoScan {
            client_options,
            db,
            collection_name: collection,
            collection: None,
            n_threads: None,
            rechunk: false,
            batch_size: None,
        })
    }

    fn get_collection(&self) -> Collection<Document> {
        let client = Client::with_options(self.client_options.clone()).unwrap();

        let database = client.database(&self.db);
        database.collection::<Document>(&self.collection_name)
    }

    fn parse_lines<'a>(
        &self,
        mut cursor: Cursor<Document>,
        buffers: &mut PlIndexMap<String, Buffer<'a>>,
    ) -> mongodb::error::Result<()> {
        while let Some(Ok(doc)) = cursor.next() {
            buffers.iter_mut().for_each(|(s, inner)| match doc.get(s) {
                Some(v) => inner.add(v).expect("was not able to add to buffer."),
                None => inner.add_null(),
            });
        }
        Ok(())
    }
}

impl AnonymousScan for MongoScan {
    fn scan(&self, scan_opts: AnonymousScanOptions) -> PolarsResult<DataFrame> {
        let collection = &self.get_collection();

        let projection = scan_opts.output_schema.clone().map(|schema| {
            let prj = schema
                .iter_names()
                .map(|name| (name.clone(), Bson::Int64(1)));

            Document::from_iter(prj)
        });

        let mut find_options = FindOptions::default();
        find_options.projection = projection;
        find_options.batch_size = self.batch_size.map(|b| b as u32);

        

        let schema = scan_opts.output_schema.unwrap_or(scan_opts.schema);

        // if no n_rows we need to get the count from mongo.
        let n_rows = scan_opts
            .n_rows
            .unwrap_or_else(|| collection.estimated_document_count(None).unwrap() as usize);

        // if n_rows is not `none`
        let n_rows_num = scan_opts.n_rows.unwrap_or(0);
        if n_rows_num > 0 {
            find_options.sort = Some(doc! {"_id": -1});
        }
        
        let mut n_threads = self.n_threads.unwrap_or_else(|| POOL.current_num_threads());

        if n_rows < 128 {
            n_threads = 1
        }

        let rows_per_thread = n_rows / n_threads;

        let dfs = POOL.install(|| {
            (0..n_threads)
                .into_par_iter()
                .map(|idx| {
                    let mut find_options = find_options.clone();

                    let start = idx * rows_per_thread;

                    find_options.skip = Some(start as u64);
                    find_options.limit = Some(rows_per_thread as i64);
                    let cursor = collection.find(None, Some(find_options));
                    let mut buffers = init_buffers(schema.as_ref(), rows_per_thread)?;

                    self.parse_lines(cursor.unwrap(), &mut buffers)
                        .map_err(|err| PolarsError::ComputeError(format!("{:#?}", err).into()))?;

                    DataFrame::new(
                        buffers
                            .into_values()
                            .map(|buf| buf.into_series())
                            .collect::<PolarsResult<_>>()?,
                    )
                })
                .collect::<PolarsResult<Vec<_>>>()
        })?;
        let mut df = accumulate_dataframes_vertical(dfs)?;

        if self.rechunk {
            df.rechunk();
        }

        if n_rows_num > 0 {
            // re-sort the result if the `n_rows` is set.
            let df_reverse = df.sort(["_id"], false)?;
            return Ok(df_reverse);
        }

        Ok(df)
    }

    fn schema(&self, infer_schema_length: Option<usize>) -> PolarsResult<Schema> {
        let collection = self.get_collection();

        let infer_options = FindOptions::builder()
            .limit(infer_schema_length.map(|i| i as i64))
            .build();

        println!("{:?}", infer_options);

        let res = collection
            .find(None, Some(infer_options))
            .map_err(|err| PolarsError::ComputeError(format!("{:#?}", err).into()))?;
        let iter = res.map(|doc| {
            let val = doc.unwrap();
            val.into_iter()
                .map(|(key, value)| {
                    let dtype = Wrap::<DataType>::from(&value);
                    (key, dtype.0)
                })
                .collect()
        });
        let schema = infer_schema(iter, infer_schema_length.unwrap_or(100));
        Ok(schema)
    }

    fn allows_predicate_pushdown(&self) -> bool {
        true
    }
    fn allows_projection_pushdown(&self) -> bool {
        true
    }
    fn allows_slice_pushdown(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct MongoScanOptions {
    /// mongodb style connection string. `mongodb://<user>:<password>@host.domain`
    pub connection_str: String,
    /// the name of the mongodb database
    pub db: String,
    /// the name of the mongodb collection
    pub collection: String,
    // Number of rows used to infer the schema. Defaults to `100` if not provided.
    pub infer_schema_length: Option<usize>,
    /// Number of rows to return from mongodb collection. If not provided, it will fetch all rows from collection.
    pub n_rows: Option<usize>,
    /// determines the number of records to return from a single request to mongodb
    pub batch_size: Option<usize>,
}

pub trait MongoLazyReader {
    fn scan_mongo_collection(options: MongoScanOptions) -> PolarsResult<LazyFrame> {
        let f = MongoScan::new(options.connection_str, options.db, options.collection)?;

        let args = ScanArgsAnonymous {
            name: "MONGO SCAN",
            infer_schema_length: options.infer_schema_length,
            n_rows: options.n_rows,
            ..ScanArgsAnonymous::default()
        };

        LazyFrame::anonymous_scan(Arc::new(f), args)
    }
}

impl MongoLazyReader for LazyFrame {}
