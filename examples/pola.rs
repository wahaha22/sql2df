use anyhow::Result;
use polars::prelude::*;
use polars::df;
use reqwest;
use std::io::Cursor;

#[tokio::main]
async fn main() -> Result<()> {

    let df = df![
        "a" => [1, 2, 3],
        "b" => [None, Some("a"), Some("b")]
    ]?;

    let filtered = df.lazy()
        .filter(col("a").gt(lit(2)))
        .collect()?;


    let url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv";
    let data = reqwest::get(url).await?.text().await?;

    let df = CsvReader::new(Cursor::new(data))
        .infer_schema(Some(16))
        .finish().unwrap();
    
        // let mask = col("new_deaths").gt(500);
        let mask = &df.column("new_deaths")?.gt(500)?;
        let filtered = df.filter(mask)?;
    println!(
        "{:?}", filtered.select([ "location", "total_cases", "new_cases", "total_deaths", "new_deaths" ])
    );
    Ok(())
}