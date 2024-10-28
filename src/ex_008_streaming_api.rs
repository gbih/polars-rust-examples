use polars::prelude::*;
use crate::utilities::*;
use crate::print_function;

//-----
// Template:
// pub fn ex001() -> PolarsResult<DataFrame> {
//     let df = df![
//         "letters" => ('a'..'z').map(|x| x.to_string()).collect::<Vec<String>>(),
//         "numeric" => (0..25).collect::<Vec<i32>>()
//     ]?;
//     print_function!();
//     print_type(&df);
//     println!("{:?}", df.dtypes());
//     println!("{:?}", df);
//     Ok(df)
// }
//
// fn ex002(df: &DataFrame) -> PolarsResult<DataFrame> {
//     let df_out = df.clone().lazy().collect()?;
//     print_function!();
//     print_type(&df_out);
//     println!("{:?}", df_out);
//     Ok(df_out)
// }
//-----

pub fn ex001() -> PolarsResult<DataFrame> {
    let query = LazyCsvReader::new("src/iris.csv")
        .with_has_header(true)
        .finish()? // get the final LazyFrame
        .filter(col("sepal_length").gt(lit(5)))
        .group_by(vec![col("species")])
        .agg([
            col("sepal_width").mean()
        ])
        .sort(["sepal_width"], Default::default());

    let df = query.clone().with_streaming(true).collect()?;

    print_function!();
    println!("To tell Polars we want to execute a query in streaming mode we pass the streaming=True argument to collect()");
    println!("Note that we have to activate the `streaming` polars feature in Cargo.toml");
    print_type(&df);
    println!("{:?}", df);

    Ok(df)

}


pub fn ex002() -> PolarsResult<DataFrame> {
    print_function!();

    let query = LazyCsvReader::new("src/iris.csv")
        .with_has_header(true)
        .finish()? // get the final LazyFrame
        .filter(col("sepal_length").gt(lit(5)))
        .group_by(vec![col("species")])
        .agg([
            col("sepal_width").mean()
        ])
        .sort(["sepal_width"], Default::default());

    let query_plan = query.clone().with_streaming(true).explain(true)?;
    println!("To determine which parts of your query are streaming, use the explain method.");
    println!("query_plan:\n{}", query_plan);
    hr3();

    let df = query.clone().with_streaming(true).collect()?;
    print_type(&df);
    println!("{:?}", df);

    Ok(df)

}



pub fn ex003() -> PolarsResult<DataFrame> {
    print_function!();

    let query = LazyCsvReader::new("src/iris.csv")
        .finish()?
        .with_columns(
            vec![col("sepal_length")
            .mean()
            .over(vec![col("species")])
            .alias("sepal_length_mean")
        ]);

    let query_plan = query.clone().with_streaming(true).explain(true)?;
    println!();
    println!("Example with non-streaming operations, both .mean() and .over().");
    println!("This is because both operations require looking at the full dataset.");
    println!("query_plan:\n{}", query_plan);
    hr3();


    let df = query.clone().with_streaming(true).collect()?;
    print_type(&df);
    println!("{:?}", df.head(Some(3)));
    Ok(df)
}


//-----

pub fn run(flag: Option<&str>) {
    println!("008 Streaming API examples");
    println!("https://docs.pola.rs/user-guide/concepts/streaming/");
    println!();

    let result01 = ex001().unwrap();
    hr2();
    pause();

    let result02 = ex002().unwrap();
    hr2();
    pause();

    let result03 = ex003().unwrap();
}
