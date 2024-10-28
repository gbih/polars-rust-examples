use crate::print_function;
use crate::utilities::*;
use my_proc_macro::print_source;
use polars::prelude::*;

//----------

#[print_source]
fn ex001() -> PolarsResult<DataFrame> {
    let df = df!(
        "value" => &[Some(1), None]
    )?;
    print_function!();
    print_data(&df);
    Ok(df)
}

// Missing data metadata
#[print_source]
fn ex002(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .null_count()
        .collect()?;
    print_function!();
    print_data(&out);
    Ok(out)
}

// Return a Series
#[print_source]
fn ex003(df: &DataFrame) -> PolarsResult<DataFrame> {
    let is_null_series = df
        .clone()
        .lazy()
        .select([col("value").is_null()])
        .collect()?;

    print_function!();
    print_data(&is_null_series);
    Ok(is_null_series.clone())
}

// Filling missing data
// Create new sample DataFrame for following examples
#[print_source]
fn ex004() -> PolarsResult<DataFrame> {
    let df = df!(
        "col1" => &[Some(1), Some(2), Some(3)],
        "col2" => &[Some(1), None, Some(3)],
    )?;
    print_function!();
    print_data(&df);
    Ok(df)
}

// Fill missing data with a specified literal value with lit()
#[print_source]
fn ex005(df: &DataFrame) -> PolarsResult<DataFrame> {
    let fill_literal_df = df
        .clone()
        .lazy()
        .with_columns([
            col("col2").fill_null(lit(2))
        ])
        .collect()?;

    print_function!();
    print_data(&fill_literal_df);
    Ok(fill_literal_df)
}

// Fill with a strategy, such as filling forward
#[print_source]
fn ex006(df: &DataFrame) -> PolarsResult<DataFrame> {
    let fill_forward_df = df
        .clone()
        .lazy()
        .with_columns([
            col("col2").forward_fill(None)
        ])
        .collect()?;
    print_function!();
    print_data(&fill_forward_df);
    Ok(fill_forward_df)
}


// Fill withh an expression
// Here, fill nulls with median value from that column
#[print_source]
fn ex007(df: &DataFrame) -> PolarsResult<DataFrame> {
    let fill_median_df = df
        .clone()
        .lazy()
        .with_columns([
            col("col2").fill_null(median("col2"))
        ])
        .collect()?;
    print_function!();
    print_data(&fill_median_df);
    Ok(fill_median_df)
}


// Fill with interpolation, without using the fill_null function
// Using .interpolate requires this addition to Cargo.toml:
// polars = { version = "0.42.0", features = [ "interpolate" ]}
#[print_source]
fn ex008(df: &DataFrame) -> PolarsResult<DataFrame> {
    let fill_interpolation_df = df
        .clone()
        .lazy()
        .with_columns([
            col("col2").interpolate(InterpolationMethod::Linear)
        ])
        .collect()?;
    print_function!();
    print_data(&fill_interpolation_df);
    Ok(fill_interpolation_df)
}

// NotaNumber or Nan values
#[print_source]
fn ex009() -> PolarsResult<DataFrame> {
    let nan_df = df!(
        "value" => &[1.0, f64::NAN, f64::NAN, 3.0]
    )?;
    print_function!();
    print_data(&nan_df);
    Ok(nan_df)
}


// Using fill_nan
#[print_source]
fn ex010(df: &DataFrame) -> PolarsResult<DataFrame> {
    let mean_nan_df = df
        .clone()
        .lazy()
        .with_columns([
            col("value").fill_nan(lit(NULL)).alias("value")
        ])
        .mean()
        .collect()?;
    print_function!();
    print_data(&mean_nan_df);
    Ok(mean_nan_df)
}
//----------

fn run_all() {
    clear_screen();
    hr2();
    let df = ex001().unwrap();
    // pause();

    clear_screen();
    hr2();
    let result002 = ex002(&df).unwrap();

    clear_screen();
    hr2();
    let result003 = ex003(&df).unwrap();

    clear_screen();
    hr2();
    let df2 = ex004().unwrap();

    clear_screen();
    hr2();
    let result005 = ex005(&df2).unwrap();

    clear_screen();
    hr2();
    let result006 = ex006(&df2).unwrap();

    clear_screen();
    hr2();
    let result007 = ex007(&df2).unwrap();

    clear_screen();
    hr2();
    let result008 = ex008(&df2).unwrap();

    clear_screen();
    hr2();
    let nan_df = ex009().unwrap();

    clear_screen();
    hr2();
    let result010 = ex010(&nan_df).unwrap();
}

fn run_individually() {
    clear_screen();
    hr2();
    let df = ex001().unwrap();
    // pause();

    clear_screen();
    hr2();
    let result002 = ex002(&df).unwrap();

    clear_screen();
    hr2();
    let result003 = ex003(&df).unwrap();

    clear_screen();
    hr2();
    let df2 = ex004().unwrap();

    clear_screen();
    hr2();
    let result005 = ex005(&df2).unwrap();

    clear_screen();
    hr2();
    let result006 = ex006(&df2).unwrap();

    clear_screen();
    hr2();
    let result007 = ex007(&df2).unwrap();

    clear_screen();
    hr2();
    let result008 = ex008(&df2).unwrap();

    clear_screen();
    hr2();
    let nan_df = ex009().unwrap();

    clear_screen();
    hr2();
    let result010 = ex010(&nan_df).unwrap();
}

//----------

pub fn run(flag: Option<&str>) {
    println!("015 Missing data examples");
    println!("https://docs.pola.rs/user-guide/expressions/missing-data/");

    if let Some(arg) = flag {
        println!("Running examples individually.\n");
        run_individually();
    } else {
        println!("Running all examples.\n");
        run_all();
    }
}
