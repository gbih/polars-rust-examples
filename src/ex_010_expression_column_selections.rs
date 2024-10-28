use polars::prelude::*;
use crate::utilities::*;
use crate::print_function;
use my_proc_macro::print_source;
use chrono::prelude::*;
use indoc::indoc;

// Create DataFrame for examples
#[print_source]
pub fn ex001() -> PolarsResult<DataFrame> {
    let df = df!(
        "id" => &[9, 4, 2],
        "place" => &["Mars", "Earth", "Saturn"],
        "date" => date_range(
            "date".into(),
            NaiveDate::from_ymd_opt(2022, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2022, 1, 3).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            Duration::parse("1d"),
            ClosedWindow::Both,
            TimeUnit::Milliseconds, None)?,
        "sales" => &[33.4, 2142134.1, 44.7],
        "has_people" => &[false, true, false],
        "logged_at" => date_range(
            "logged_at".into(),
            NaiveDate::from_ymd_opt(2022, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2022, 1, 1).unwrap().and_hms_opt(0, 0, 2).unwrap(),
            Duration::parse("1s"),
            ClosedWindow::Both,
            TimeUnit::Milliseconds,
            None)?,
        )?
        .with_row_index("index".into(), None)?;

    print_function!();
    print_type(&df);
    println!("{:?}", df);

    Ok(df)

}


// API: all
#[print_source]
pub fn ex002(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .select([
            col("*")
        ])
        .collect()?;

    print_function!();
    print_type(&out);
    println!("{:?}", out);

    Ok(out)
}


// API: exclude
#[print_source]
pub fn ex003(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .select([
            col("*").exclude(["logged_at", "index"])
        ])
        .collect()?;

    print_function!();
    print_type(&out);
    println!("{:?}", out);

    Ok(out)
}

// By multiple strings
// Specifying multiple strings allows expressions to expand to all matching columns
#[print_source]
pub fn ex004(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .select([
            cols(["date", "logged_at"]).dt().to_string("%Y-%h-%d")
        ])
        .collect()?;

    print_function!();
    print_type(&out);
    println!("{:?}", out);

    Ok(out)
}


// By regular expressions
#[print_source]
pub fn ex005(df: &DataFrame) -> PolarsResult<DataFrame> {
    let notes = indoc! {r#"

    Note that regex this to work, you need to have the "regex" feature enabled
    in your Polars dependency. Make sure your Cargo.toml includes:
    dependencies]
    polars = { version = "...", features = ["lazy", "regex"] }

    The string pattern "^.(as|sa).$" is a regular expression that
    selects columns containing "as" or "sa" anywhere in their names.
    The ^ and $ anchors ensure that the entire column name is matched.
    "#};

    println!("{}", notes);

    let out = df
        .clone()
        .lazy()
        .select([
            col("^.*(as|sa).*$")
        ])
        .collect()?;


    print_function!();
    print_type(&out);
    println!("{:?}", out);

    Ok(out)
}


// By data type
// pl.col can select multiple columns using Polars data types.
#[print_source]
fn ex006(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .select([
            dtype_cols([
                DataType::Int64,
                DataType::UInt32,
                DataType::Boolean
            ]).n_unique()
        ])
        .collect()?;

    print_function!();
    print_type(&out);
    println!("{:?}", out);

    Ok(out)
}


/*
Using selectors
https://docs.pola.rs/user-guide/expressions/column-selections/#debugging-selectors
Not available in Rust, refer the following link
https://github.com/pola-rs/polars/issues/10594
For now, the polars.selectors module is implemented in Python-only.

https://github.com/pola-rs/polars/blob/main/py-polars/polars/selectors.py
*/

//----------

fn run_all() {
    clear_screen();
    let df = ex001().unwrap();
    hr2();

    let result02 = ex002(&df).unwrap();
    hr2();

    let result03 = ex003(&df).unwrap();
    hr2();

    let result04 = ex004(&df).unwrap();
    hr2();

    let result05 = ex005(&df).unwrap();
    hr2();

    let result = ex006(&df).unwrap();
    hr2();
}

fn run_individually() {

    let df = ex001().unwrap();
    hr2();
    pause();

    clear_screen();
    let result02 = ex002(&df).unwrap();
    hr2();
    pause();

    clear_screen();
    let result03 = ex003(&df).unwrap();
    hr2();
    pause();

    clear_screen();
    let result04 = ex004(&df).unwrap();
    hr2();
    pause();

    clear_screen();
    let result05 = ex005(&df).unwrap();
    hr2();
    pause();

    clear_screen();
    let result = ex006(&df).unwrap();
    hr2();
}

//----------

pub fn run(flag: Option<&str>) {
    clear_screen();
    println!("010 Expressions: Column Selection examples");
    println!("https://docs.pola.rs/user-guide/expressions/column-selections/");

    if let Some(arg) = flag {
        println!("Running examples individually.");
        run_individually();
    } else {
        println!("Running all examples.");
        run_all();
    }
}
