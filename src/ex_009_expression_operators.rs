use polars::prelude::*;
use crate::utilities::*;
use crate::print_function;

use rand::{thread_rng, Rng};

// Create DataFrame for expression examples
pub fn ex001() -> PolarsResult<DataFrame> {
    let mut arr = [0f64; 5];
    thread_rng().fill(&mut arr);

    let df = df!(
        "nrs" => &[Some(1), Some(2), Some(3), None, Some(5)],
        "names" => &[Some("foo"), Some("ham"), Some("spam"), Some("eggs"), None],
        "random" => &arr,
        "groups" => &["A", "A", "B", "C", "B"],
    )?;

    print_function!();
    print_type(&df);
    println!("{:?}", df.head(Some(3)));

    Ok(df)
}


// Numerical expression
pub fn ex002(df: &DataFrame) -> PolarsResult<DataFrame> {
    let df_numerical = df.clone().lazy()
        .select([
            (col("nrs") + lit(5)).alias("nrs + 5"),
            (col("nrs") - lit(5)).alias("nrs - 5"),
            (col("nrs") * col("random")).alias("nrs * random"),
            (col("nrs") / col("random")).alias("nrs / random"),
        ])
        .collect()?;

    print_function!();
    print_type(&df_numerical);
    println!("{:?}", df_numerical.head(Some(3)));
    Ok(df_numerical)
}


// Logical expression
pub fn ex003(df: &DataFrame) -> PolarsResult<DataFrame> {
    let df_logical = df
        .clone()
        .lazy()
        .select([
            col("nrs").gt(1).alias("nrs > 1"),
            col("random").lt_eq(0.5).alias("random < .5"),
            col("nrs").neq(1).alias("nrs != 1"),
            col("nrs").eq(1).alias("nrs == 1"),
            (col("random").lt_eq(0.5)).and(col("nrs").gt(1)).alias("and_expr"),
            (col("random").lt_eq(0.5).or(col("nrs").gt(1))).alias("or_expr"),
        ])
        .collect()?;

    print_function!();
    print_type(&df_logical);
    println!("{:?}", df_logical);
    Ok(df_logical)
}






//-----
pub fn run(flag: Option<&str>) {
    println!("009 Expressions: Operators examples");
    println!("https://docs.pola.rs/user-guide/expressions/operators/");
    println!();

    // Create DataFrame for examples
    let df = ex001().unwrap();
    hr2();
    // pause();

    // // Numerical
    // let result02 = ex002(&df).unwrap();
    // hr2();
    // // pause();

    // Logical
    let result03 = ex003(&df).unwrap();

}
