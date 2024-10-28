use polars::prelude::*;
use crate::utilities::*;
use crate::print_function;
use chrono::prelude::*;
use indoc::indoc;
use my_proc_macro::print_source;

//---------------

#[print_source]
pub fn ex001() -> PolarsResult<DataFrame> {
    let df = df!(
        "integers" => &[1,2,3,4,5],
        "big_integers" => &[1, 10000002, 3, 10000004, 10000005],
        "floats" => &[4.0, 5.0, 6.0, 7.0, 8.0],
        "floats_with_decimal" => &[4.532, 5.5, 6.5, 7.5, 8.5],
    )?;
    print_function!();
    print_type(&df);
    println!("{:?}", df);
    Ok(df)
}


// Perform casting operations between floats and integers
#[print_source]
pub fn ex002(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .select([
            col("integers")
                .cast(DataType::Float32)
                .alias("integers_as_floats"),
            col("floats")
                .cast(DataType::Int32)
                .alias("floats_as_integers"),
            col("floats_with_decimal")
                .cast(DataType::Int32)
                .alias("floats_with_decimal_as_integers")
        ])
        .collect()?;

    print_function!();
    print_type(&out);
    println!("{:?}", out);

    Ok(out)

}


// Downcast
// Casting from Int64 to Int16 and Float64 to Float32 to reduce memory usage
#[print_source]
pub fn ex003(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .select([
            col("integers")
                .cast(DataType::Int16)
                .alias("integers_smallfootprint"),
            col("floats")
                .cast(DataType::Float32)
                .alias("floats_smallfootprint")
        ])
        .collect();

    print_function!();
    print_type(&out);

    match &out {
        Ok(out) => println!("{:?}", out),
        Err(e) => println!("{:?}", e)
    }

    out

}


// Overflow
#[print_source]
pub fn ex004(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .select([
            col("big_integers")
                .strict_cast(DataType::Int8)
        ])
        .collect();

    print_function!();
    print_type(&out);
    println!();

    match &out {
        Ok(out) => println!("{:?}", out),
        Err(e) => println!("{:?}", e)
    }

    out
}


// You can also set the strict parameter to false,
// which converts values that are overflowing to null values.
// Overflow with strict parameter to false
#[print_source]
pub fn ex005(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .select([
            col("big_integers")
                .cast(DataType::Int8)
        ])
        .collect();

    print_function!();
    print_type(&out);
    println!();

    match &out {
        Ok(out) => println!("{:?}", out),
        Err(e) => println!("{:?}", e)
    }

    out
}


// Strings
// Strings can be casted to numerical data types and vice versa.
#[print_source]
pub fn ex006() -> PolarsResult<DataFrame> {
    let df = df!(
        "integers" => &[1, 2, 3, 4, 5],
        "float" => &[4.0, 5.03, 6.0, 7.0, 8.0],
        "floats_as_string" => &["4.0", "5.0", "6.0", "7.0", "8.0"],
    )?;

    let out = df
        .clone()
        .lazy()
        .select([
            col("integers").cast(DataType::String),
            col("float").cast(DataType::String),
            (col("floats_as_string").cast(DataType::Float64))
        ])
        .collect()?;

    print_function!();
    print_type(&out);
    println!("{:?}", out);
    Ok(out)
}


// In case the column contains a non-numerical value, Polars will throw
// a ComputeError detailing the conversion error.
// Setting strict=False will convert the non float value to null.
#[print_source]
pub fn ex007() -> PolarsResult<DataFrame> {
    let df = df! (
        "strings_not_float"=> ["4.0", "not_a_number", "6.0", "7.0", "8.0"]
    )?;

    let out = df
        .clone()
        .lazy()
        .with_columns([
            //col("strings_not_float").strict_cast(DataType::Float64).alias("strict_cast"),
            col("strings_not_float").cast(DataType::Float64).alias("cast(strict=False)"),
        ])
        .collect();

    match &out {
        Ok(out) => println!("{:?}", out),
        Err(e) => println!("{:?}", e)
     }

    print_function!();
    print_type(&out);
    println!("{:?}", out);

    out
}


// Booleans
// It's possible to perform casting operations between a numerical DataType and a boolean, and vice versa.
// However, casting from a string (String) to a boolean is not permitted.
#[print_source]
pub fn ex008() -> PolarsResult<DataFrame> {
    let df = df! (
        "integers"=> &[-1, 0, 2, 3, 4],
        "floats"=> &[0.0, 1.0, 2.0, 3.0, 4.0],
        "bools"=> &[true, false, true, false, true],
    )?;

    let out = df
        .clone()
        .lazy()
        .with_columns([
            col("integers").cast(DataType::Boolean).alias("integers2bool"),
            col("floats").cast(DataType::Boolean).alias("floats2bool"),
        ])
        .select([
            col("integers"),
            col("integers2bool"),
            col("floats"),
            col("floats2bool"),
            col("bools")
        ])
        .collect()?;

    print_function!();
    print_type(&out);
    println!("{:?}", out);

    Ok(out)
}


// Dates
/*
Temporal data types such as Date or Datetime are represented as the number
of days (Date) and microseconds (Datetime) since epoch. Therefore, casting
between the numerical types and the temporal data types is allowed.
*/
#[print_source]
pub fn ex009() -> PolarsResult<DataFrame> {
    let date = polars::time::date_range(
        "date".into(),
        NaiveDate::from_ymd_opt(2022, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        NaiveDate::from_ymd_opt(2022, 1, 5)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        Duration::parse("1d"),
        ClosedWindow::Both,
        TimeUnit::Milliseconds,
        None,
    )?
    .cast(&DataType::Date)?;

    let datetime = polars::time::date_range(
        "datetime".into(),
        NaiveDate::from_ymd_opt(2022, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        NaiveDate::from_ymd_opt(2022, 1, 5)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        Duration::parse("1d"),
        ClosedWindow::Both,
        TimeUnit::Milliseconds,
        None,
    )?;

    let df = df!(
        "date" => date,
        "datetime" => datetime,
    )?;

    let out = df
        .clone()
        .lazy()
        .with_columns([
            col("date").cast(DataType::Int64).alias("date_cast"),
            col("datetime").cast(DataType::Int64).alias("datetime_cast"),
        ])
        .select([
            col("date"),
            col("date_cast"),
            col("datetime"),
            col("datetime_cast"),
        ])
        .collect()?;

    print_function!();
    print_type(&out);
    println!("{:?}", out);

    Ok(out)

}


// Converting between strings and Dates / Datetimes,
// using dt.to_string and str.to_datetime
// Need to enable the `strings` feature for the polars crate in
// Cargo.toml:
// [dependencies]
// polars = { version = "...", features = ["lazy", "strings"] }
#[print_source]
pub fn ex010() -> PolarsResult<DataFrame> {
    let date = polars::time::date_range(
        "date".into(),
        NaiveDate::from_ymd_opt(2022, 1, 1)
            .unwrap()
            .and_hms_opt(0,0,0)
            .unwrap(),
        NaiveDate::from_ymd_opt(2022, 1, 5)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        Duration::parse("1d"),
        ClosedWindow::Both,
        TimeUnit::Milliseconds,
        None,
    )?;

    let df = df!(
        "date" => date,
        "string" => &[
            "2022-01-01",
            "2022-01-02",
            "2022-01-03",
            "2022-01-04",
            "2022-01-05",
        ],
    )?;

    let out = df
        .clone()
        .lazy()
        .select([
            col("date").dt().to_string("%Y-%m-%d"),
            col("string").str().to_datetime(
                Some(TimeUnit::Microseconds),
                None,
                StrptimeOptions::default(),
                lit("raise")
            ),
        ])
        .collect()?;

    print_function!();
    print_type(&out);
    println!("{:?}", out);

    Ok(out)

}
//----------

fn run_all() {
    // Sample DataFrame for examples
    hr2();
    let df = ex001().unwrap();
    pause();

    clear_screen();
    hr2();
    let result02 = ex002(&df).unwrap();
    pause();

    clear_screen();
    hr2();
    let result03 = ex003(&df).unwrap();
    pause();

    clear_screen();
    hr2();
    let result04 = ex004(&df);
    pause();

    clear_screen();
    hr2();
    let result05 = ex005(&df).unwrap();
    pause();

    clear_screen();
    hr2();
    let result06 = ex006().unwrap();
    pause();

    clear_screen();
    hr2();
    let result07 = ex007().unwrap();
    pause();

    clear_screen();
    hr2();
    let result08 = ex008().unwrap();
    pause();

    clear_screen();
    hr2();
    let result09 = ex009().unwrap();
    pause();

    clear_screen();
    hr2();
    let result10 = ex010().unwrap();
}

fn run_individually() {
    // Sample DataFrame for examples
    hr2();
    let df = ex001().unwrap();
    pause();

    clear_screen();
    hr2();
    let result02 = ex002(&df).unwrap();
    pause();

    clear_screen();
    hr2();
    let result03 = ex003(&df).unwrap();
    pause();

    clear_screen();
    hr2();
    let result04 = ex004(&df);
    pause();

    clear_screen();
    hr2();
    let result05 = ex005(&df).unwrap();
    pause();

    clear_screen();
    hr2();
    let result06 = ex006().unwrap();
    pause();

    clear_screen();
    hr2();
    let result07 = ex007().unwrap();
    pause();

    clear_screen();
    hr2();
    let result08 = ex008().unwrap();
    pause();

    clear_screen();
    hr2();
    let result09 = ex009().unwrap();
    pause();

    clear_screen();
    hr2();
    let result10 = ex010().unwrap();
}

//----------

pub fn run(flag: Option<&str>) {
    println!("012 Casting examples");
    println!("https://docs.pola.rs/user-guide/expressions/casting/");

    if let Some(arg) = flag {
        println!("Running examples individually.\n");
        run_individually();
    } else {
        println!("Running all examples.\n");
        run_all();
    }
}
