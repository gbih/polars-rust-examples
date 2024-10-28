use crate::print_function;
use crate::utilities::*;
use my_proc_macro::print_source;
use polars::prelude::*;
// use reqwest::blocking::Client;
use indoc::indoc;

//----------

// Create sample DataFrame for following examples
#[print_source]
fn ex001() -> PolarsResult<DataFrame> {
    let ratings = df!(
        "Movie"=> &["Cars", "IT", "ET", "Cars", "Up", "IT", "Cars", "ET", "Up", "ET"],
        "Theatre"=> &["NE", "ME", "IL", "ND", "NE", "SD", "NE", "IL", "IL", "SD"],
        "Avg_Rating"=> &[4.5, 4.4, 4.6, 4.3, 4.8, 4.7, 4.7, 4.9, 4.7, 4.6],
        "Count"=> &[30, 27, 26, 29, 31, 28, 28, 26, 33, 26],
    )?;

    print_function!();
    print_data(&ratings);
    Ok(ratings)
}


// Struct type

const NOTES_EX002: &str = indoc! {r#"
pub fn value_counts()
Available on crate feature dtype-struct only.
Count all unique values and create a struct mapping value to count. (Note that it is better to turn parallel off in the aggregation context).
https://docs.pola.rs/api/rust/dev/polars_lazy/dsl/enum.Expr.html#method.value_counts

Need to enable crate feature `dtype-struct` in Cargo.toml:

polars = { version = "0.42.0", features = [ "dtype-struct"]}

"#};
#[print_source]
fn ex002(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .select([
            col("Theatre")
                .value_counts(
                    true, // sort: bool
                    true, // parallel: bool
                    "count".to_string(), // name: String
                    false // normalize: bool
                )
        ])
        .collect()?;

    print_function!();
    print_data(&out);
    println!("\nNOTES\n{}", NOTES_EX002);
    Ok(out)
}


// Unnest the given Struct columns.
// The fields of the Struct type will be inserted as columns.
#[print_source]
fn ex003(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .select([
            col("Theatre")
                .value_counts(
                    true, // sort: bool
                    true, // parallel: bool
                    "count".to_string(), // name: String
                    false // normalize: bool
                )
        ])
        .unnest(["Theatre"])
        .collect()?;

    print_function!();
    print_data(&out);
    Ok(out)
}


// Structs as dicts
// Polars will interpret a dict sent to the Series constructor as a Struct
#[print_source]
fn ex004() -> PolarsResult<Series> {
    let ratings_series = df!(
        "Movie" => &["Cars","Toy Story"],
        "Theatre" => &["NE", "ME"],
        "Avg_Rating" => &[4.5, 4.9],
    )?
    .into_struct("ratings".into())
    .into_series();

    print_function!();
    print_type(&ratings_series);
    println!("DATA      : {:?}", &ratings_series);
    Ok(ratings_series)
}



//----------

fn run_all() {
    clear_screen();
    hr2();
    let df_ratings = ex001().unwrap();

    hr2();
    let result002 = ex002(&df_ratings).unwrap();

    hr2();
    let result003 = ex003(&df_ratings).unwrap();

    hr2();
    let ratings_series = ex004().unwrap();
}

fn run_individually() {
    clear_screen();
    hr2();
    let df_ratings = ex001().unwrap();
    pause();

    clear_screen();
    hr2();
    let result002 = ex002(&df_ratings).unwrap();
    pause();

    clear_screen();
    hr2();
    let result003 = ex003(&df_ratings).unwrap();
    pause();

    clear_screen();
    hr2();
    let ratings_series = ex004().unwrap();
    pause();

}


//----------

pub fn run(flag: Option<&str>) {
    println!("019 Struct datatype examples");
    println!("https://docs.pola.rs/user-guide/expressions/structs/");

    if let Some(arg) = flag {
        println!("Running examples individually.\n");
        run_individually();
    } else {
        println!("Running all examples.\n");
        run_all();
    }
}
