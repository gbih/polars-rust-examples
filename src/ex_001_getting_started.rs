use polars::prelude::*;
use std::fs::File;
use chrono::prelude::*; // need to add this crate via cargo

use crate::utilities::*;
use crate::print_function;

//-----


// # Example
// https://docs.pola.rs/

pub fn ex000() -> PolarsResult<()> { // alias for `Result<T, PolarsError>`
    // https://docs.pola.rs/
    let q = LazyCsvReader::new("src/iris.csv")
    .with_has_header(true) // specifies the CSV has a header row
    .finish()?// finalizes the LazyCsvReader config
    .filter(col("sepal_length").gt(lit(5)))// apply lazy operations
    .group_by(vec![col("species")])// apply lazy operations
    .agg([col("*").sum()]); // apply lazy operations

    let df = q.collect()?; // execute lazy query and collects results into DataFrame

    print_function!();
    print_type(&df);
    println!("{:?}", df);

    Ok(())
}

//-------------------------------------

// # Reading & writing
// https://docs.pola.rs/user-guide/getting-started/#reading-writing

// Alias for `Result<T, PolarsError>`
// Wrap the returned DataFrame in a PolarsResult to handle errors
// This allows using the ? operator for error propagation and consistent way to handle errors

// Type Alias polars::error::PolarsResult
// https://docs.pola.rs/api/rust/dev/polars/error/type.PolarsResult.html

pub fn ex001() -> PolarsResult<DataFrame> {

    let date_series = Series::new("datetime",
    &[
        NaiveDate::from_ymd_opt(2025, 12, 1).unwrap().and_hms_opt(0,0,0).unwrap(),
        NaiveDate::from_ymd_opt(2025, 12, 2).unwrap().and_hms_opt(0,0,0).unwrap(),
        NaiveDate::from_ymd_opt(2025, 12, 3).unwrap().and_hms_opt(0,0,0).unwrap(),
        NaiveDate::from_ymd_opt(2025, 12, 4).unwrap().and_hms_opt(0,0,0).unwrap(),
        NaiveDate::from_ymd_opt(2025, 12, 5).unwrap().and_hms_opt(0,0,0).unwrap(),
    ]);

    let mut df: DataFrame = df!(
        "a" => &[0,1,2,3,4],
        "b" => &[0.927457, 0.997467, 0.015117, 0.170232, 0.696244],
        "c" => date_series,
        "d" => &[Some(1.0), Some(2.0), Some(f64::NAN), Some(-42.0), None],
        "d2" => &[1.0, 2.0, f64::NAN, -42.0, f64::NAN],
    )
    .unwrap();

    let out = df.lazy()
        .with_columns([
        //.select([
           // col("integer").cast(DataType::String),

           // TimeUnit::Milliseconds, Microseconds
            col("c").cast(DataType::Datetime(TimeUnit::Nanoseconds, None)),
           // col("float").cast(DataType::Float64),
           // col("string").cast(DataType::String)
        ])
        .collect()?;

    print_function!();
    print_type(&out);
    println!("{:?}", out);

    Ok(out)
}


// https://docs.pola.rs/user-guide/getting-started/#reading-writing
// .finish(): https://www.perplexity.ai/search/in-rust-polars-what-is-finish-NjK4jodNR9SCd2vxn_XQ.Q

pub fn ex002(df_input: &DataFrame) -> PolarsResult<DataFrame> {
    let mut file = File::create("src/output.csv")
                    .expect("Could not create file");
    let df = &mut df_input.clone();

    CsvWriter::new(&mut file)
        .include_header(true)
        .with_separator(b',')
        .finish(df)?;

    let df_csv = CsvReadOptions::default()
        .with_infer_schema_length(None)
        .with_has_header(true)
        .try_into_reader_with_file_path(Some("src/output.csv".into()))?
        .finish()?;

    print_function!();
    print_type(&df_csv);
    // Here, datetime is cast to string when we write to CSV
    println!("{}", df_csv);

    Ok(df_csv)
}

//-------------------------------------

// # Expressions

// * Select
// https://docs.pola.rs/user-guide/getting-started/#select

pub fn ex003(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df.clone()
                .lazy()
                .select([col("*")])
                .collect()?;

    print_function!();
    print_type(&out);
    println!("{:?}", out);
    Ok(out)
}


// Specifying the specific columns to return via select
pub fn ex004(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df.clone()
                .lazy()
                .select([col("a"), col("b")])
                .collect()?;

    print_function!();
    print_type(&out);
    println!("{:#?}", out);
    Ok(out)
}


// * Filter
// https://docs.pola.rs/user-guide/getting-started/#filter

pub fn ex005(df: &DataFrame) -> PolarsResult<DataFrame> {
    let start_date = NaiveDate::from_ymd_opt(2025, 12, 2)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
    let end_date = NaiveDate::from_ymd_opt(2025, 12, 3)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
    let out = df
                .clone()
                .lazy()
                .filter(
                    col("c")
                        .gt_eq(lit(start_date))
                        .and(col("c").lt_eq(lit(end_date))),
                )
                .collect()?;

    print_function!();
    print_type(&out);
    println!("{:#?}", out);

    Ok(out)
}


// More complex filters that include multiple columns

pub fn ex006(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
                .clone()
                .lazy()
                .filter(
                    col("a").lt_eq(3).and(col("d").is_not_null().and(col("d2").is_not_nan()))
                )
                .collect()?;

    print_function!();
    print_type(&out);
    println!("{:#?}", out);
    Ok(out)
}


// * Add columns
// https://docs.pola.rs/user-guide/getting-started/#add-columns

pub fn ex007(df_input: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df_input
            .clone()
            .lazy()
            .with_columns([
                col("b").sum().alias("e"),
                (col("b") + lit(42)).alias("b+42")
            ])
            .collect()?;

    print_function!();
    print_type(&out);
    println!("{:?}", out);

    Ok(out)
}


// * Group by
// https://docs.pola.rs/user-guide/getting-started/#group-by

pub fn ex008() -> PolarsResult<DataFrame> {
    let df2: DataFrame = df!(
        "x" => 0..8,
        "y" => &["A", "A", "A", "B", "B", "C", "X", "X"]
    )
    .expect("should not fail");

    print_function!();
    print_type(&df2);
    println!("{:?}", df2);
    Ok(df2)
}


pub fn ex009(df_input: &DataFrame) -> PolarsResult<DataFrame> {

    let out = df_input
                .clone()
                .lazy()
                .group_by(["y"])
                .agg([len()])
                .collect()?;

    print_function!();
    print_type(&out);
    println!("{:?}", out);
    Ok(out)
}


pub fn ex010(df_input: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df_input
                .clone()
                .lazy()
                .group_by(["y"])
                .agg([
                    col("*").count().alias("count"),
                    col("*").sum().alias("sum")
                ])
                .collect()?;

    print_function!();
    print_type(&out);
    println!("{:?}", out);
    Ok(out)
}


// * Combination
// https://docs.pola.rs/user-guide/getting-started/#combination

pub fn ex011(df_input: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df_input
            .clone()
            .lazy()
            .with_columns([
                (col("a") * col("b")).alias("a * b")
            ])
            .select([
                col("*").exclude(["c", "d", "d2"])
            ])
            .collect()?;

    print_function!();
    print_type(&out);
    println!("{:?}", out);
    Ok(out)
}


pub fn ex012(df_input: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df_input
                .clone()
                .lazy()
                .with_columns([
                    (col("a") * col("b")).alias("a * b")
                ])
                .select([
                    col("*").exclude(["d", "d2"])
                ])
                .collect()?;

    print_function!();
    print_type(&out);
    println!("{:?}", out);
    Ok(out)
}

//---------------------------------------


// # Combining DataFrames

use rand::Rng;

// create df3
pub fn ex013() -> PolarsResult<DataFrame> {
    let mut rng = rand::thread_rng();
    let df3: DataFrame = df!(
        "a" => 0..8,
        "b" => (0..8).map(|_| rng.gen::<f64>()).collect::<Vec<f64>>(),
        "d" => [Some(1.0), Some(2.0), Some(f64::NAN), Some(f64::NAN), Some(0.0), Some(-5.0), Some(-42.0), None]
    )?;

    print_function!();
    print_type(&df3);
    println!("{:?}", df3);
    Ok(df3)
}


// create df4
pub fn ex014() -> PolarsResult<DataFrame> {
    let df4: DataFrame = df!(
        "x" => 0..8,
        "y" => &["A", "A", "A", "B", "B", "C", "X", "X"]
    )?;

    print_function!();
    print_type(&df4);
    println!("{:?}", df4);
    Ok(df4)
}


// Join
// https://docs.pola.rs/user-guide/getting-started/#join
pub fn ex015(df3: &DataFrame, df4: &DataFrame) -> PolarsResult<DataFrame> {
    let joined = df3.join(&df4, ["a"], ["x"], JoinType::Left.into())?;

    print_function!();
    print_type(&joined);
    println!("{:?}", joined);
    Ok(joined)
}


// Concat
// https://docs.pola.rs/user-guide/getting-started/#concat
pub fn ex016(df3: &DataFrame, df4: &DataFrame) -> PolarsResult<DataFrame> {
    let stacked = df3.hstack(df4.get_columns())?;
    print_function!();
    print_type(&stacked);
    println!("{:?}", stacked);
    Ok(stacked)
}

//---------------------------------

pub fn run(flag: Option<&str>) {
    println!("001 Getting Started examples");
    println!("https://docs.pola.rs/user-guide/getting-started/");

    // let df_csv = ex000().unwrap();
    // hr2();
    let df1 = ex001().unwrap(); // create df1 for expressions
    hr2();
    pause();

    let result2 = ex002(&df1).unwrap(); // write to file
    hr2();
    pause();

    let result3 = ex003(&df1).unwrap(); // expression: select all
    hr2();
    pause();

    let result4 = ex004(&df1).unwrap(); // expression: select
    hr2();
    pause();

    let result5 = ex005(&df1).unwrap(); // expression: simple filter
    hr2();
    let result6 = ex006(&df1).unwrap(); // expression: complicated filter
    hr2();
    pause();

    let result7 = ex007(&df1).unwrap(); // expression: add columns
    hr2();
    pause();


    let df2 = ex008().unwrap(); // create df1 for group_by
    hr2();
    let result9 = ex009(&df2).unwrap(); // group_by
    hr2();
    let result10 = ex010(&df2).unwrap(); // group_by
    hr2();
    pause();


    let result11 = ex011(&df1).unwrap(); // combination
    hr2();
    let result12 = ex012(&df1).unwrap(); // combination
    hr2();
    pause();


    let df3 = ex013().unwrap(); // create df3 for join
    hr2();
    pause();
    let df4 = ex014().unwrap(); // create df4 for join
    hr2();
    pause();
    let result15 = ex015(&df3, &df4).unwrap(); // join
    hr2();
    pause();
    let result16 = ex016(&df3, &df4).unwrap(); // concat
}
