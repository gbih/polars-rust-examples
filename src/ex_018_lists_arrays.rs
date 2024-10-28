use crate::print_function;
use crate::utilities::*;
use my_proc_macro::print_source;
use polars::prelude::*;
// use reqwest::blocking::Client;
use indoc::indoc;

//----------

// Create sample DataFrame for examples
#[print_source]
fn ex001() -> PolarsResult<DataFrame> {
    let stns: Vec<String> = (1..6)
            .map(|i| format!("Station {i}"))
            .collect();
    let weather = df!(
        "station" => &stns,
        "temperatures" => &[
            "20 5 5 E1 7 13 19 9 6 20",
            "18 8 16 11 23 E2 8 E2 E2 E2 90 70 40",
            "19 24 E9 16 6 12 10 22",
            "E2 E0 15 7 8 10 E1 24 17 13 6",
            "14 8 E0 16 22 24 E1",
        ]
    )?;

    print_function!();
    print_data(&weather);
    Ok(weather)
}

// Creating a List column
// Extract individual data from 'temperatures' column with str().split()
#[print_source]
fn ex002(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .with_columns([
            col("temperatures").str().split(lit(" "))
        ])
        .collect()?;

    print_function!();
    print_data(&out);
    Ok(out)
}


// Explore data into own row
#[print_source]
fn ex003(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .with_columns([
            col("temperatures").str().split(lit(" "))
        ])
        .explode(["temperatures"])
        .collect()?;

    print_function!();
    print_data(&out);
    Ok(out)
}


// Operating on List columns
// head, tail, slice, lenghts operations
#[print_source]
fn ex004(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .with_columns([
            col("temperatures")
                .str()
                .split(lit(" "))
        ])
        .with_columns([
            col("temperatures")
                .list()
                .head(lit(3))
                .alias("top3"),
            col("temperatures")
                .list()
                .slice(lit(-3), lit(3)) // same as .tail(lit(3))
                .alias("bottom_3"),
            col("temperatures")
                .list()
                .len()
                .alias("obs")
        ])
        .collect()?;

    print_function!();
    print_data(&out);
    Ok(out)

}


// Element-wise computation with List s
// Count the null values per station
// Note:
// https://docs.pola.rs/api/rust/dev/polars_lazy/dsl/trait.ListNameSpaceExtension.html#method.eval
// Need to enable crate feature list_eval in Cargo.toml

const NOTES_EX005: &str = indoc! {r#"
Trait polars_lazy::dsl::ListNameSpaceExtension

The ListNameSpaceExtension trait in Polars' lazy API provides additional methods for working with list columns. This trait extends the functionality of the ListNameSpace struct, which represents specialized expressions for Series of DataType::List.

Need to enable crate feature list_eval in Cargo.toml, eg:
    polars = { version = "0.42.0", features = [
    	"lazy", "list_eval",
    ]}

Some methods provided by this trait include:

    eval: Evaluates an expression for each element in the list.
    to_struct: Converts a list to a struct.
    lengths: Returns the length of each list.
    sum: Computes the sum of elements in each list.
    max: Finds the maximum value in each list.
    min: Finds the minimum value in each list.
    mean: Calculates the mean of elements in each list.
    sort: Sorts the elements within each list.
    reverse: Reverses the order of elements in each list.
    unique: Keeps only unique values in each list.
    get: Retrieves an element at a specific index from each list.
    join: Joins the elements of each list into a single string.
"#};

#[print_source]
fn ex005(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .with_columns([
            col("temperatures")
            .str()
            .split(lit(" "))
            .list()
            .eval(col("").cast(DataType::Int64).is_null(), false)
            .list()
            .sum()
            .alias("errors")])
        .collect()?;

    print_function!();
    print_data(&out);
    println!("\nNOTES\n{}", NOTES_EX005);
    Ok(out)
}


// Using .list().eval() with regex
// Recognize the presence of any alphabetical character
#[print_source]
fn ex006(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .with_columns([
            col("temperatures")
            .str()
            .split(lit(" "))
            .list()
            .eval(
                // Overall: Checks if each element contains any lowercase or uppercase letter (case-insensitive regex).
                // col(""): This creates a column expression. The empty string "" is used as a placeholder,
                // since named columns are not allowed in `list.eval`
                col("")
                    .str()
                    // (?i) makes the regex case-insensitive
                    // [a-z] matches any single letter from a to z
                    // false:  pattern should not be treated as a literal string, but as a regex
                    .contains(lit("(?i)[a-z]"), false), false
            )
            .list() //  Ensures the result is still a list column.
            .sum()
            .alias("errors")
        ])
        .collect()?;

    print_function!();
    print_data(&out);
    Ok(out)
}


// Row-wise computations
// Ideal for computing in row orientation
// We can apply any Polars operations on the elements of the list with the list.eval (list().eval in Rust) expression
// New DataFrame for the following examples
#[print_source]
fn ex007() -> PolarsResult<DataFrame> {
    let stns: Vec<String> = (1..11)
            .map(|i| format!("Station {i}"))
            .collect();

    let weather_by_day = df!(
        "station" => &stns,
        "day_1" => &[17, 11, 8, 22, 9, 21, 20, 8, 8, 17],
        "day_2" => &[15, 11, 10, 8, 7, 14, 18, 21, 15, 13],
        "day_3" => &[16, 15, 24, 24, 8, 23, 19, 23, 16, 10],
    )?;

    print_function!();
    print_data(&weather_by_day);
    Ok(weather_by_day)
}



const NOTES_EX008: &str = indoc! {r#"
Calculate percentage rank of the temperatures by day, measured across stations
Create percentage rank expression for highest temperature.

Necessary features:

Struct polars_lazy::prelude::RankOptions
https://docs.pola.rs/api/rust/dev/polars_lazy/prelude/struct.RankOptions.html
Need to enable crate feature `rank` in Cargo.toml, eg:
    polars = { version = "0.42.0", features = [ "rank"]}

Enum polars::prelude::Expr
pub fn round(self, decimals: u32) -> Expr
https://docs.rs/polars/latest/polars/prelude/enum.Expr.html#method.round
https://docs.rs/polars-plan/0.43.1/src/polars_plan/dsl/mod.rs.html
Need to enable crate feature `round_series` in Cargo.toml, eg:
    polars = { version = "0.42.0", features = [ "round_series"]}

"#};
#[print_source]
fn ex008(df: &DataFrame) -> PolarsResult<DataFrame> {
    let rank_pct = (
        col("")
        .rank(
            RankOptions {
                method: RankMethod::Average,
                descending: true,
            },
            None,
        )
        .cast(DataType::Float32) / col("*").count().cast(DataType::Float32))
        .round(2);

    let out = df
        .clone()
        .lazy()
        .with_columns([
            // create the list of homogeneous data
            concat_list([all().exclude(["station"])])?.alias("all_temps")
        ])
        .select([
            // select all columns except the intermediate list
            all().exclude(["all_temps"]),
            // compute the rank by calling `list.eval`
            col("all_temps")
                .list()
                .eval(rank_pct, true)
                .alias("temps_rank"),
        ])
        .collect()?;

    print_function!();
    print_data(&out);
    println!("\nNOTES\n{}", NOTES_EX008);
    Ok(out)

}


// Polars Arrays
fn ex009() -> PolarsResult<DataFrame> {
    let mut col1: ListPrimitiveChunkedBuilder<Int32Type> = ListPrimitiveChunkedBuilder::new("Array_1".into(), 8, 8, DataType::Int32);
    col1.append_slice(&[1, 3]);
    col1.append_slice(&[2, 5]);
    let mut col2: ListPrimitiveChunkedBuilder<Int32Type> =
        ListPrimitiveChunkedBuilder::new("Array_2".into(), 8, 8, DataType::Int32);
    col2.append_slice(&[1, 7, 3]);
    col2.append_slice(&[8, 1, 0]);

    let array_df = DataFrame::new(vec![
        col1.finish(),
        col2.finish(),
    ])?;

    print_function!();
    print_data(&array_df);
    Ok(array_df)
}


// Running basic operations on Polars Arrays
#[print_source]
fn ex010(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .select([
            col("Array_1"),
            col("Array_1").list().min().name().suffix("_min"),
            col("Array_2").list().min().name().suffix("_sum"),
            col("Array_2"),
        ])
        .collect()?;

    print_function!();
    print_data(&out);
    Ok(out)
}



//----------

fn run_all() {
    clear_screen();
    hr2();
    let df = ex001().unwrap();

    hr2();
    let result002 = ex002(&df).unwrap();

    hr2();
    let result003 = ex003(&df).unwrap();

    hr2();
    let result004 = ex004(&df).unwrap();

    hr2();
    let result005 = ex005(&df).unwrap();

    hr2();
    let result006 = ex006(&df).unwrap();

    hr2();
    let df_weather = ex007().unwrap();

    hr2();
    let result008 = ex008(&df_weather).unwrap();

    hr2();
    let df_array = ex009().unwrap();

    hr2();
    let result010 = ex010(&df_array).unwrap();
}

fn run_individually() {
    clear_screen();
    hr2();
    let df = ex001().unwrap();
    pause();

    clear_screen();
    hr2();
    let result002 = ex002(&df).unwrap();
    pause();

    clear_screen();
    hr2();
    let result003 = ex003(&df).unwrap();
    pause();

    clear_screen();
    hr2();
    let result004 = ex004(&df).unwrap();
    pause();

    clear_screen();
    hr2();
    let result005 = ex005(&df).unwrap();
    pause();

    clear_screen();
    hr2();
    let result006 = ex006(&df).unwrap();
    pause();

    clear_screen();
    hr2();
    let df_weather = ex007().unwrap();
    pause();

    clear_screen();
    hr2();
    let result008 = ex008(&df_weather).unwrap();
    pause();

    clear_screen();
    hr2();
    let df_array = ex009().unwrap();
    pause();

    clear_screen();
    hr2();
    let result010 = ex010(&df_array).unwrap();
    pause();


}

//----------

pub fn run(flag: Option<&str>) {
    println!("018 List and Arrays examples");
    println!("https://docs.pola.rs/user-guide/expressions/lists/");

    if let Some(arg) = flag {
        println!("Running examples individually.\n");
        run_individually();
    } else {
        println!("Running all examples.\n");
        run_all();
    }
}
