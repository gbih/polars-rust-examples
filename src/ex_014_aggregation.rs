use crate::print_function;
use crate::utilities::*;
use my_proc_macro::print_source;
use polars::prelude::*;

use std::io::Cursor;

//---------------

use std::fs::File;
use std::io::copy;
use std::path::Path;

use reqwest::blocking::Client;

// utility function download_file defined in utilities
#[print_source]
pub fn ex001() -> PolarsResult<DataFrame> {
    let url = "https://theunitedstates.io/congress-legislators/legislators-historical.csv";
    let output_path = "./src/ex_014_aggregation_legislators-historical.csv";

    match download_file(url, output_path) {
        Ok(_) => println!("Process completed for file: {:?}\n", output_path),
        Err(e) => eprintln!("Error: {:?}\n", e),
    }

    let mut data: Vec<u8> = Client::new()
        .get(url)
        .send()
        .map_err(|e| PolarsError::ComputeError(format!("Failed to send request: {}", e).into()))?
        .text()
        .map_err(|e| PolarsError::ComputeError(format!("Failed to get text: {}", e).into()))?
        .bytes()
        .collect();

    let mut dataset = CsvReadOptions::default()
        .with_has_header(true)
        .with_infer_schema_length(Some(100))
        .map_parse_options(|parse_options| parse_options.with_try_parse_dates(true))
        .try_into_reader_with_file_path(Some(output_path.into()))?
        .finish()?;

    // Modify specific columns to categorical
    let categorical_columns = [
        "first_name",
        "last_name",
        "gender",
        "type",
        "state",
        "party",
    ];
    for col in categorical_columns.iter() {
        if let Ok(series) = dataset.column(col) {
            let categorical = series.cast(&DataType::Categorical(None, Default::default()))?;
            dataset.replace(col, categorical)?;
        }
    }

    // Convert birthday column to Date type if it exists
    if let Ok(series) = dataset.column("birthday") {
        let date_series = series.cast(&DataType::Date)?;
        dataset.replace("birthday", date_series)?;
    }

    let reduced_df = dataset.select([
        "first_name",
        "last_name",
        "gender",
        "type",
        "state",
        "party",
        "birthday",
    ])?;

    print_function!();
    print_data(&reduced_df);
    Ok(reduced_df)
}

// List of aggregate functions:
// https://docs.rs/polars/latest/polars/?search=Agg
// 1. Sum: `col("column_name").sum()`
// 2. Mean: `col("column_name").mean()`
// 3. Median: `col("column_name").median()`
// 4. Min: `col("column_name").min()`
// 5. Max: `col("column_name").max()`
// 6. Count: `col("column_name").count()`
// 7. First: `col("column_name").first()`
// 8. Last: `col("column_name").last()`
// 9. Standard deviation: `col("column_name").std()`
// 10. Variance: `col("column_name").var()`
// 11. Quantile: `col("column_name").quantile(0.5)` (for 50th percentile)
// 12. Unique count: `col("column_name").n_unique()`
// 13. List: `col("column_name").list()`
// 14. Custom aggregations using `apply()` or `map_batches()`

// Basic aggregations (group_by, agg)

//#[print_source]
pub fn ex002(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .group_by(["first_name"])
        .agg([
            len(),
            col("first_name").count().alias("count"),
            col("gender"),
            col("last_name").first(),
        ])
        .sort(
            ["len"],
            SortMultipleOptions::default()
                .with_order_descending(true)
                .with_nulls_last(true),
        )
        .limit(5)
        .collect()?;

    print_function!();
    print_data(&out);

    Ok(out)
}

// Conditionals
#[print_source]
pub fn ex003(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .group_by(["state"])
        .agg([
            (col("party").eq(lit("Anti-Administration")))
                .sum()
                .alias("anti"),
            (col("party").eq(lit("Pro-Administration")))
                .sum()
                .alias("pro"),
        ])
        .sort(
            ["pro"],
            SortMultipleOptions::default().with_order_descending(true),
        )
        .limit(5)
        .collect()?;

    print_function!();
    print_data(&out);
    Ok(out)
}

// nested group by
#[print_source]
pub fn ex004(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .group_by(["state", "party"])
        .agg([col("party").count().alias("count")])
        .filter(
            col("party")
                .eq(lit("Anti-Administration"))
                .or(col("party").eq(lit("Pro-Administration"))),
        )
        .sort(
            ["count"],
            SortMultipleOptions::default()
                .with_order_descending(true)
                .with_nulls_last(true),
        )
        .limit(5)
        .collect()?;

    print_function!();
    print_data(&out);
    Ok(out)
}

// Filtering groups
#[print_source]
pub fn ex005(df: &DataFrame) -> PolarsResult<DataFrame> {
    fn compute_age() -> Expr {
        lit(2022) - col("birthday").dt().year()
    }

    fn avg_birthday(gender: &str) -> Expr {
        compute_age()
            .filter(col("gender").eq(lit(gender)))
            .mean()
            // The alias() method expects a &str (string slice), but format! returns a String
            // The as_str() method creates a string slice that refers to the entire String,
            // allowing it to be passed to functions expecting &str without allocating a new string.
            // Convert the String returned by format! to a &str via .as_str()
            .alias(format!("avg {} birthday", gender).as_str())
    }

    let out = df
        .clone()
        .lazy()
        .group_by(["state"])
        .agg([
            avg_birthday("M"),
            avg_birthday("F"),
            (col("gender").eq(lit("M"))).sum().alias("# male"),
            (col("gender").eq(lit("F"))).sum().alias("# female"),
        ])
        .limit(5)
        .collect()?;

    print_function!();
    print_data(&out);

    Ok(out)
}

// Sorting
#[print_source]
pub fn ex006(df: &DataFrame) -> PolarsResult<DataFrame> {
    fn get_person() -> Expr {
        col("first_name") + lit(" ") + col("last_name")
    }

    let out = df
        .clone()
        .lazy()
        .sort(
            ["birthday"],
            SortMultipleOptions::default()
                .with_order_descending(true)
                .with_nulls_last(true),
        )
        .group_by(["state"])
        .agg([
            get_person().first().alias("youngest"),
            col("birthday").first().alias("youngest birthday"),
            get_person().last().alias("oldest"),
            col("birthday").last().alias("oldest birthday"),
        ])
        .limit(5)
        .collect()?;

    print_function!();
    print_data(&out);

    Ok(out)
}

// Sorting names alphabetically, in group_by context separate from the DataFrame.
#[print_source]
pub fn ex007(df: &DataFrame) -> PolarsResult<DataFrame> {
    fn get_person() -> Expr {
        col("first_name") + lit(" ") + col("last_name")
    }

    let out = df
        .clone()
        .lazy()
        .sort(
            ["birthday"],
            SortMultipleOptions::default()
                .with_order_descending(true)
                .with_nulls_last(true),
        )
        .group_by(["state"])
        .agg([
            get_person().first().alias("youngest"),
            get_person().last().alias("oldest"),
            get_person()
                .sort(Default::default())
                .first()
                .alias("alphabetical_first"),
        ])
        .limit(5)
        .collect()?;

    print_function!();
    print_data(&out);

    Ok(out)
}

// utility function
fn get_person() -> Expr {
    col("first_name") + lit(" ") + col("last_name")
}

// Sort by another column in the group_by context.
// If we want to know if the alphabetically sorted name is male or female we could add:
// pl.col("gender").sort_by(get_person()).first()
#[print_source]
pub fn ex008(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .sort(
            ["birthday"],
            SortMultipleOptions::default()
                .with_order_descending(true)
                .with_nulls_last(true),
        )
        .group_by(["state"])
        .agg([
            get_person().first().alias("youngest"),
            get_person().last().alias("oldets"),
            get_person()
                .sort(Default::default())
                .first()
                .alias("alphabetical_first"),
            col("gender")
                .sort_by(["first_name"], SortMultipleOptions::default())
                .first()
                .alias("gender"),
        ])
        .sort(["state"], SortMultipleOptions::default())
        .limit(5)
        .collect()?;

    print_function!();
    print_data(&out);
    Ok(out)
}

//----------

fn run_all() {
    // Sample DataFrame for examples
    hr2();
    let df = ex001().unwrap();
    //pause();

    // Basic aggregations
    clear_screen();
    hr2();
    let result002 = ex002(&df).unwrap();
    pause();

    // Conditionals
    clear_screen();
    hr2();
    let result = ex003(&df).unwrap();
    pause();

    // Nested group by
    clear_screen();
    hr2();
    let result = ex004(&df).unwrap();
    pause();

    // Nested group by
    clear_screen();
    hr2();
    let result = ex005(&df).unwrap();
    pause();

    // Sorting
    clear_screen();
    hr2();
    let result = ex006(&df).unwrap();
    pause();

    // Sorting 2
    clear_screen();
    hr2();
    let result007 = ex007(&df).unwrap();

    // Sorting 3
    clear_screen();
    hr2();
    let result008 = ex008(&df).unwrap();
}

fn run_individually() {
    // Sample DataFrame for examples
    hr2();
    let df = ex001().unwrap();
    //pause();

    // Basic aggregations
    clear_screen();
    hr2();
    let result002 = ex002(&df).unwrap();
    pause();

    // Conditionals
    clear_screen();
    hr2();
    let result = ex003(&df).unwrap();
    pause();

    // Nested group by
    clear_screen();
    hr2();
    let result = ex004(&df).unwrap();
    pause();

    // Nested group by
    clear_screen();
    hr2();
    let result = ex005(&df).unwrap();
    pause();

    // Sorting
    clear_screen();
    hr2();
    let result = ex006(&df).unwrap();
    pause();

    // Sorting 2
    clear_screen();
    hr2();
    let result007 = ex007(&df).unwrap();

    // Sorting 3
    clear_screen();
    hr2();
    let result008 = ex008(&df).unwrap();
}

//----------


pub fn run(flag: Option<&str>) {
    println!("014 Aggregation examples");
    println!("https://docs.pola.rs/user-guide/expressions/aggregation/");

    if let Some(arg) = flag {
        println!("Running examples individually.\n");
        run_individually();
    } else {
        println!("Running all examples.\n");
        run_all();
    }
}
