use polars::prelude::*;
use crate::utilities::*;
use crate::print_function;

pub fn ex001() -> PolarsResult<DataFrame> {
    let df = df!(
        "foo" => 0..10,
        "bar" => 100..110
    )?;

    print_function!();
    print_type(&df);
    println!("{:?}", df);

    Ok(df)
}

pub fn ex002(df: &DataFrame) -> PolarsResult<DataFrame> {
    // For more complex operations, try converting to a LazyFrame first
    let df_out = df.clone().lazy()
        .select([
            col("foo")
            .sort(Default::default())
            .head(Some(2))
            ])
        .collect()?;

    print_function!();
    print_type(&df_out);
    println!("{:?}", df_out);

    Ok(df_out)
}

/*
While the .clone() operation might seem potentially expensive,
it's generally quite cheap for Polars DataFrames. Internally,
Polars uses reference counting for its data structures, so cloning
a DataFrame typically just increments a few reference counters
rather than copying large amounts of data.

If you're certain you won't need the original DataFrame afterwards,
you can often omit the .clone() and just use df.lazy() directly.
However, the .clone().lazy() pattern is a safe default that
preserves the original DataFrame while allowing for optimized
lazy operations.
https://www.perplexity.ai/search/in-rust-polars-why-is-it-commo-Gv4maHZGQkWN47sFAEnVIA


https://github.com/pola-rs/polars/blob/py-1.7.1/py-polars/polars/dataframe/frame.py#L7707-L7740
Create a copy of this DataFrame.
This is a cheap operation that does not copy data.

https://www.linkedin.com/posts/liam-brannigan-9080b214a_polarsdataframeclone-polars-documentation-activity-7112028471739965441-eNRm/
In Pandas you learn that copying a DataFrame is something to be avoided because it copies all the data. In PolarsLand things are very different, however - copying a DataFrame is almost free.
When you copy a Pandas DataFrame you copy all the data. When you copy a Polars DataFrame you essentially create a new reference to the existing data. At the same time changes to a column in your new dataframe won't affect your original dataframe
It's free until you modify it - only then will the data be copied
*/

// Run two expressions via df.select
fn ex003(df: &DataFrame) -> PolarsResult<DataFrame> {

    //let df_out = df.clone().lazy().collect()?;

    let df_out = df.clone().lazy()
        .select([
            col("foo").sort(Default::default()).head(Some(2)),
            col("bar").filter(col("foo").eq(lit(1))).sum(),
        ])
        .collect()?;

    print_function!();
    print_type(&df_out);
    println!("{:?}", df_out);

    Ok(df_out)
}





//-----

pub fn run(flag: Option<&str>) {
    println!("006 Expression examples");
    println!("https://docs.pola.rs/user-guide/concepts/expressions/");

    let df = ex001().unwrap();
    hr2();
    pause();

    let result02 = ex002(&df).unwrap();
    hr2();
    pause();

    let result03 = ex003(&df).unwrap();

}
