use anyhow::{anyhow, Result};
use std::io::Cursor;

use polars::prelude::*;
use sqlparser::{ast, parser::Parser};
use tracing::info;

mod dialect;
mod convert;
mod fetcher;

pub use dialect::URLDialect;
use convert::*;
use fetcher::*;

pub async fn query(sql: String) -> Result<DataFrame> {
    let ast = Parser::parse_sql(&URLDialect::default(), sql.as_str()).unwrap();

    if ast.len() != 1 {
        return Err(anyhow!("Only support single sql!"));
    }

    let sqlop: SQLOperation = ast[0].clone().try_into()?;
    let dfop: DataFrameOperation = sqlop.try_into()?;

    let DataFrameOperation {
        selection,
        condition,
        source,
        order_by,
        offset,
        limit,
    } = dfop.clone();

    info!("retrieving data from source: {}", source);

    let df = load(fetch(source).await?)?;

    // 1. 过滤
    let mut filtered = match condition {
        Some(expr) => df.lazy().filter(expr),
        None => df.lazy()
    };

    // 2. 排序
    filtered = order_by
        .into_iter()
        .fold(filtered, |acc, (a, b)| acc.sort(&a, SortOptions { descending: b, nulls_last: true }));

    // 3. offset, limit
    filtered = filtered.slice(offset.unwrap_or(0), limit.unwrap_or(u32::MAX) );
    // 4. select
    let res = filtered.select(selection).collect()?;

    Ok(res)
}

fn load(data: String) -> Result<DataFrame> {
    //TODO: detect content

    let df = CsvReader::new(Cursor::new(data)).infer_schema(Some(16)).finish()?;
    Ok(df)
}