use polars::prelude::*;
use rand::{thread_rng, Rng};
use crate::utilities::*;
use crate::print_function;

//-----

// Create sample DaraFrame for examples
pub fn ex001() -> PolarsResult<DataFrame> {
    let mut arr = [064; 5];
    thread_rng().fill(&mut arr);

    let df = df!(
        "nrs" => &[Some(1), Some(2), Some(3), None, Some(5)],
        "names" => &[Some("foo"), Some("ham"), Some("spam"), Some("eggs"), None],
        "random" => &arr,
        "groups" => &["A", "A", "B", "C", "B"]
    )?;

    print_function!();
    print_type(&df);
    println!("{:?}", df);

    Ok(df)
}


// Selection
// https://docs.pola.rs/user-guide/concepts/contexts/#selection

pub fn ex002(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
            .clone()
            .lazy()
            .select([
                sum("nrs"),
                mean("nrs").alias("nrs_mean"),
                col("names").sort(Default::default()),
                col("names").first().alias("first name"),
                (mean("nrs") * lit(10)).alias("10xnrs")
            ])
            .collect()?;

    print_function!();
    print_type(&out);
    println!("{:?}", out);
    Ok(out)
}

// with_columns
pub fn ex003(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
                .clone()
                .lazy()
                .with_columns([
                    sum("nrs").alias("nrs_sum"),
                    col("random").count().alias("count")
                ])
                .collect()?;

    print_function!();
    print_type(&out);
    println!("{:?}", out);
    Ok(out)
}


// Filtering
// https://docs.pola.rs/user-guide/concepts/contexts/#filtering
pub fn ex004(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
                .clone()
                .lazy()
                .filter(col("nrs")
                .gt(lit(2)))
                .collect()?;

    print_function!();
    print_type(&out);
    println!("{:?}", out);
    Ok(out)
}




// Group by / aggregation
// https://docs.pola.rs/user-guide/concepts/contexts/#group-by-aggregation

pub fn ex005(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df.clone()
                .lazy()
                .group_by([col("groups")])
                .agg([
                    sum("nrs"),
                    col("random").count().alias("count"),
                    col("random")
                        .filter(col("names").is_not_null())
                        .sum()
                        .name()
                        .suffix("_sum"),
                    col("names").reverse().alias("reversed name"),
                ])
                .collect()?;

    print_function!();
    print_type(&out);
    println!("{:?}", out);

    Ok(out)
}

//-----

pub fn run(flag: Option<&str>) {
    println!("005 Context examples");
    println!("https://docs.pola.rs/user-guide/concepts/contexts/");

    let df1 = ex001().unwrap();
    hr2();
    pause();

    let result002 = ex002(&df1); //.unwrap();
    hr2();
    pause();

    let result3 = ex003(&df1); //.unwrap();
    hr2();
    pause();

    let result4 = ex004(&df1); //.unwrap();
    hr2();
    pause();

    let result5 = ex005(&df1).unwrap();
}
