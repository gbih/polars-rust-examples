use polars::prelude::*;
use crate::utilities::*;
use crate::print_function;

use rand::{thread_rng, Rng};


pub fn placeholder() {
    println!("Placeholder");
}

// Create DataFrame for examples
pub fn ex001() -> PolarsResult<DataFrame> {
    let mut arr = [0f64; 5];
    thread_rng().fill(&mut arr);

    let df = df!(
        "nrs" => &[Some(1), Some(2), Some(3), None, Some(5)],
        "names" => &["foo", "ham", "spam", "egg", "spam"],
        "random" => &arr,
        "groups" => &["A", "A", "B", "C", "B"],
    )?;

    print_function!();
    print_type(&df);
    println!("{:?}", df);

    Ok(df)
}


// Column naming
pub fn ex002(df: &DataFrame) -> PolarsResult<DataFrame> {
    let df_samename = df
        .clone()
        .lazy()
        .select([
            col("nrs") + lit(5)
        ])
        .collect()?;

    print_function!();
    print_type(&df_samename);
    println!("df_samename: {:?}", df_samename);
    Ok(df_samename)
}

// Failing query
pub fn ex003(df: &DataFrame) -> PolarsResult<DataFrame> {
    print_function!();
    let df_samename2 = df
        .clone()
        .lazy()
        .select([col("nrs") + lit(5), col("nrs") - lit(5)])
        .collect();

    match &df_samename2 {
        Ok(df_samename2) => println!("{}", &df_samename2),
        Err(e) => println!("{:#?}", &e),
    };

    let out = df_samename2?.clone();

    print_type(&out);
    println!("{:?}", out);

    Ok(out)
}


// Passing query via changing the output name with alias()
pub fn ex004(df: &DataFrame) -> PolarsResult<DataFrame> {
    let df_alias = df
        .clone()
        .lazy()
        .select([
            (col("nrs") + lit(5)).alias("nrs + 5"),
            (col("nrs") - lit(5)).alias("nrs - 5"),
        ])
        .collect()?;

    print_function!();
    print_type(&df_alias);
    println!("{:?}", df_alias);

    Ok(df_alias)
}

// Count unique values
// .n_unique() give the number of unique rows (as a scalar)
// .approx_n_unique() is deprecated and removed:
// https://github.com/pola-rs/polars/issues/13498
pub fn ex005(df: &DataFrame) -> PolarsResult<DataFrame> {
    let df_alias = df
        .clone()
        .lazy()
        .select([
            col("names").n_unique().alias("unique"),
        ])
        .collect()?;

    print_function!();
    print_type(&df_alias);
    println!("{:?}", df_alias);

    Ok(df_alias)
}


// Conditionals
/*
Polars supports if-else like conditions in expressions with
the when, then, otherwise syntax. The predicate is placed
in the when clause and when this evaluates to true the then
expression is applied otherwise the otherwise expression is
applied (row-wise).
*/
pub fn ex006(df: &DataFrame) -> PolarsResult<DataFrame> {
    let df_conditional = df
        .clone()
        .lazy()
        .select([
            col("nrs"),
            when(col("nrs").gt(2))
                .then(lit(true))
                .otherwise(lit(false))
                .alias("conditional")
        ])
        .collect()?;

    print_function!();
    print_type(&df_conditional);
    println!("{:?}", df_conditional);

    Ok(df_conditional)
}


//----------

fn run_all() {
    // Sample DataFrame for examples
    let df = ex001().unwrap();
    hr2();
    pause();

    // Column naming
    let result02 = ex002(&df).unwrap();
    hr2();
    pause();

    // Failing query
    let result03 = ex003(&df);
    hr2();
    pause();

    // Passing query
    let result04 = ex004(&df).unwrap();
    hr2();
    pause();

    // Count unique values
    let result05 = ex005(&df).unwrap();
    hr2();
    pause();

    // Conditionals
    let result06 = ex006(&df).unwrap();

}

fn run_individually() {
    // Sample DataFrame for examples
    let df = ex001().unwrap();
    hr2();
    pause();

    // Column naming
    let result02 = ex002(&df).unwrap();
    hr2();
    pause();

    // Failing query
    let result03 = ex003(&df);
    hr2();
    pause();

    // Passing query
    let result04 = ex004(&df).unwrap();
    hr2();
    pause();

    // Count unique values
    let result05 = ex005(&df).unwrap();
    hr2();
    pause();

    // Conditionals
    let result06 = ex006(&df).unwrap();

}


//----------

pub fn run(flag: Option<&str>) {
    println!("011 Functions examples");
    println!("https://docs.pola.rs/user-guide/expressions/functions/");

    if let Some(arg) = flag {
        println!("Running examples individually.\n");
        run_individually();
    } else {
        println!("Running all examples.\n");
        run_all();
    }
}
