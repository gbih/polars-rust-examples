use polars::prelude::*;
use polars::datatypes::CategoricalOrdering::*;
use polars_arrow::array::*;

use crate::utilities::*;
use crate::print_function;

//-----

pub fn ex001() -> PolarsResult<Series> {
    print_function!();
    println!("Without string cache");

    // Check if the string cache is enabled
    println!("String cache enabled: {}\n", polars::using_string_cache());

    let mut cat1_series = Series::new("one", &["Polar", "Panda", "Brown", "Brown", "Polar"])
                            .cast(&DataType::Categorical(None, Lexical))?;

    let cat2_series = Series::new("two", &["Panda", "Brown", "Brown", "Polar", "Polar"])
                            .cast(&DataType::Categorical(None, Lexical))?;

    print_type(&cat1_series);
    println!("cat_series ::: {:?}", cat1_series);
    hr3();

    // Convert a mutable reference to a Series (&mut Series) into an owned Series
    let out = cat1_series.append(&cat2_series)?.clone();
    print_type(&out);
    println!("{:?}", out);

    Ok(out)
}






pub fn ex002() -> PolarsResult<Series> {
    print_function!();
    println!("Using string cache");

    // This string cache is disabled after _sc goes out of scope
    let _sc = StringCacheHolder::hold();

    // Check if the string cache is enabled
    println!("String cache enabled: {}\n", polars::using_string_cache());

    // Create two categorical series
    let mut cat1_series = Series::new("one", &["Polar", "Panda", "Brown", "Brown", "Polar"])
                            .cast(&DataType::Categorical(None, Lexical))?;
    let cat2_series = Series::new("two", &["Panda", "Brown", "Brown", "Polar", "Polar"])
                            .cast(&DataType::Categorical(None, Lexical))?;

    print_type(&cat1_series);
    println!("cat_series ::: {:?}", cat1_series);
    hr3();
    // Concatenate the series
    let out = cat1_series.append(&cat2_series)?.clone();

    print_type(&out);
    println!("{:?}", out);

    Ok(out)
}

//----

pub fn run(flag: Option<&str>) {
    println!("002 Categorical examples");
    println!("https://docs.pola.rs/user-guide/concepts/data-types/categoricals/#categorical-data-type");

    let cat_result1 = ex001().unwrap();
    hr2();
    pause();

    let cat_result1 = ex002().unwrap();
    println!("String cache enabled: {}", polars::using_string_cache());
}
