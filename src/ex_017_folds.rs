use crate::print_function;
use crate::utilities::*;
use my_proc_macro::print_source;
use polars::prelude::*;
// use reqwest::blocking::Client;
use indoc::indoc;

//----------

const NOTES_EX001: &str = indoc! {r#"
Polars provides expressions/methods for horizontal aggregations like sum,min, mean, etc. However, when you need a more complex aggregation the default methods Polars supplies may not be sufficient. That's when folds come in handy.

The fold expression operates on columns for maximum speed. It utilizes the data layout very efficiently and often has vectorized execution.

This snippet recursively applies the function f(acc, x) -> acc to an accumulator acc and a new column x. The function operates on columns individually and can take advantage of cache efficiency and vectorization.
"#};

// Manual sum
#[print_source]
fn ex001() -> PolarsResult<DataFrame> {
    let df = df!(
        "a" => &[1, 2, 3],
        "b" => &[10, 20, 30],
    )?;
    print_function!();
    print_data(&df);

    let out = df
        .clone()
        .lazy()
        .select([
            fold_exprs(
                lit(0),
                |acc, x| (acc + x).map(Some),
                [col("*")]
            ).alias("sum")
        ])
        .collect()?;

    print_data(&out);
    println!("\nNOTES\n{}", NOTES_EX001);
    Ok(out)
}


// Conditional
const NOTES_EX002: &str = indoc! {r#"
In the case where you'd want to apply a condition/predicate on all columns in a DataFrame a fold operation can be a very concise way to express this.

In the snippet we filter all rows where each column value is > 1.

Function polars_lazy::dsl::fold_exprs
https://docs.pola.rs/api/rust/dev/polars_lazy/dsl/fn.fold_exprs.html
Accumulate over multiple columns horizontally / row wise.

pub fn fold_exprs<F, E>(acc: Expr, f: F, exprs: E) -> Expr
where
    F: 'static + Fn(Column, Column) -> Result<Option<Column>, PolarsError> + Send + Sync + Clone,
    E: AsRef<[Expr]>,
"#};

#[print_source]
fn ex002() -> PolarsResult<DataFrame> {
    let df = df!(
        "a" => &[1, 2, 3],
        "b" => &[0, 1, 2],
    )?;

    let out = df
        .clone()
        .lazy()
        .filter(
            fold_exprs(
                lit(true), // accumulator
                |acc, x| acc.bitand(&x).map(Some), // function
                [col("*").gt(1)], // expression
            )
        )
        .collect()?;

    print_function!();
    print_data(&out);
    println!("\nNOTES\n{}", NOTES_EX002);
    Ok(out)
}


// Folds and string data
// concat_str need feature concat_str to be enabled in Cargo.toml
const NOTES_EX003: &str = indoc! {r#"
Folds could be used to concatenate string data. However, due to the materialization of intermediate columns, this operation will have squared complexity.

Therefore, we recommend using the concat_str expression for this.

.concat_str() need feature "concat_str" to be enabled in Cargo.toml
"#};

#[print_source]
fn ex003() -> PolarsResult<DataFrame> {
    let df = df!(
        "a" => &["a", "b", "c"],
        "b" => &[1, 2, 3],
    )?;
    print_function!();
    print_data(&df);

    let out = df
        .clone()
        .lazy()
        .select([
            concat_str([col("a"), col("b")], "", false)
        ])
        .collect()?;

    print_data(&out);
    println!("\nNOTES\n{}", NOTES_EX003);
    Ok(out)

}




//----------

fn run_all() {
    clear_screen();
    hr2();
    let result001 = ex001().unwrap();

    hr2();
    let result002 = ex002().unwrap();

    hr2();
    let result003 = ex003().unwrap();
}

fn run_individually() {
    clear_screen();
    hr2();
    let result001 = ex001().unwrap();
    pause();

    clear_screen();
    hr2();
    let result002 = ex002().unwrap();
    pause();

    clear_screen();
    hr2();
    let result003 = ex003().unwrap();
    pause();
}

//----------

pub fn run(flag: Option<&str>) {
    println!("017 Fold functions examples");
    println!("https://docs.pola.rs/user-guide/expressions/folds/");

    if let Some(arg) = flag {
        println!("Running examples individually.\n");
        run_individually();
    } else {
        println!("Running all examples.\n");
        run_all();
    }
}
