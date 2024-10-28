use polars::prelude::*;
use polars::datatypes::*;
use polars_arrow::*;
use chrono::NaiveDate;

use crate::utilities::*;
use crate::print_function;

//--------------------

// Series
// https://docs.pola.rs/user-guide/concepts/data-structures/#series


// polars_core::named_from::Series
// impl<T> NamedFrom for Series
// fn new(name: &str, v: T) -> Self
// where
//     // Bounds from impl:
//     T: AsRef<[i32]>,

pub fn ex001() -> PolarsResult<Series> {
    let s = Series::new("a", &[1, 2, 3, 4, 5]);

    print_function!();
    print_type(&s);
    println!("{:#?}", s);
    Ok(s)
}


// DataFrame
// https://docs.pola.rs/user-guide/concepts/data-structures/#dataframe

pub fn ex002() -> PolarsResult<DataFrame> {
    let df: DataFrame = df!(
        "integer" => &[1,2,3,4,5],
        "date" => &[
            NaiveDate::from_ymd_opt(2025, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 2).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 3).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 4).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 5).unwrap().and_hms_opt(0, 0, 0).unwrap(),
        ],
        "float" => &[4.0, 5.0, 6.0, 7.0, 8.0]
    )
    .unwrap();

    // println!("ex002, DataFrame");
    println!("{:?}", df);
    print_function!();
    print_type(&df);
    Ok(df)
}







// Viewing data
// https://docs.pola.rs/user-guide/concepts/data-structures/#viewing-data
pub fn ex003(df: DataFrame) -> PolarsResult<DataFrame> {

    // Head
    let df_head = df.head(Some(3));
    println!("df.head(3): {:?}", df_head);

    // Tail
    let df_tail = df.tail(Some(3));
    println!("df.tail(3): {:?}", df_tail);

    // Sample
    let n = Series::new("", &[2]);
    let sampled_df = df.sample_n(&n, false, false, None).unwrap();
    println!("df.sample(): {:?}", sampled_df);

    // Describe
    // TODO
    print_function!();
    print_type(&df);
    Ok(df)
}

//-----

pub fn run(flag: Option<&str>) {
    println!("004 Data Structure examples");
    println!("https://docs.pola.rs/user-guide/concepts/data-structures/");


    // data structure set: ex_004_data_structures
    let result001 = ex001().unwrap(); // Create sample Series for examples
    hr2();
    pause();

    let result002 = ex002().unwrap();
    hr2();
    pause();

    let result003 = ex003(result002).unwrap();
}
