use polars::prelude::*;
use crate::utilities::*;
use crate::print_function;

//-----
// Template:
// fn ex002(df: &DataFrame) -> PolarsResult<DataFrame> {
//     let df_out = df.clone().lazy().collect()?;
//     print_function!();
//     print_type(&df_out);
//     println!("{:?}", df_out);
//     Ok(df_out)
// }
//-----


// Eager API
pub fn ex001() -> PolarsResult<DataFrame> {
    let df = CsvReadOptions::default()
        .try_into_reader_with_file_path(Some("src/iris.csv".into()))
        .unwrap()
        .finish()
        .unwrap();
    print_function!();
    println!("df: {:?}", df);
    hr3();

    let mask = df.column("sepal_length")?.f64()?.gt(5.0);
    println!("mask: {:?}", mask);
    hr3();

    let df_small = df.filter(&mask)?;
    println!("df_small: {:?}", df_small);
    hr3();

    // old api
    #[allow(deprecated)]
    let df_agg = df_small
        .group_by(["species"])?
        .select(["sepal_width"])
        .mean()?
        .sort(["sepal_width_mean"], Default::default())?;
    print_type(&df_agg);
    println!("df_agg_old: {:?}", df_agg);

    Ok(df_agg)

}


// Lazy API
pub fn ex002() -> PolarsResult<DataFrame> {
    // Define query
    let q = LazyCsvReader::new("src/iris.csv")
        .with_has_header(true)
        .finish()?
        .filter(col("sepal_length").gt(lit(5)))
        .group_by(vec![col("species")])
        .agg([col("sepal_width").mean().alias("sepal_width_mean")])
        .sort(["sepal_width_mean"], Default::default());

    // Execute query
    let df = q.collect()?;

    print_function!();
    print_type(&df);
    println!("df: {:?}", df);
    Ok(df)

}


//-----

pub fn run(flag: Option<&str>) {
    println!("007 Lazy / Eager API examples");
    println!("https://docs.pola.rs/user-guide/concepts/lazy-vs-eager/");
    println!();

    let result01 = ex001().unwrap();
    hr2();
    pause();

    let result02 = ex002().unwrap();
    hr2();
}
