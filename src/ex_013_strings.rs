use polars::prelude::*;
use crate::utilities::*;
use crate::print_function;
use my_proc_macro::print_source;

//---------------

// Sample DataFrame for examples
// Using the string namespace via .column attribute `str`
#[print_source]
pub fn ex001() -> PolarsResult<DataFrame> {
    let df = df!(
        "animal" => &[Some("Crab"), Some("cat and dog"), Some("rab$bit"), None],
    )?;

    print_function!();
    print_data(&df);
    Ok(df)
}


// len_bytes and len_chars
#[print_source]
pub fn ex002(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .with_columns([
            col("animal").str().len_bytes().alias("byte_count"),
            col("animal").str().len_chars().alias("letter_count"),
        ])
        .collect()?;

    print_function!();
    print_data(&out);
    Ok(out)
}


// String parsing
#[print_source]
pub fn ex003(df: &DataFrame) -> PolarsResult<DataFrame> {

    let out = df
        .clone()
        .lazy()
        .select([
            col("animal"),
            col("animal")
                .str()
                .contains(lit("cat|bit"), false)
                .alias("regex"),
            col("animal")
                .str()
                .contains_literal(lit("rab$"))
                .alias("literal"),
            col("animal")
                .str()
                .starts_with(lit("rab"))
                .alias("starts_with"),
            col("animal")
                .str()
                .ends_with(lit("dog"))
                .alias("ends_with"),
        ])
        .collect()?;

    print_function!();
    print_data(&out);
    Ok(out)
}


// Extract a pattern
#[print_source]
pub fn ex004() -> PolarsResult<DataFrame> {
    let df = df!(
        "a" => &[
            "http://vote.com/ballon_dor?candidate=messi&ref=polars",
            "http://vote.com/ballon_dor?candidat=jorginho&ref=polars",
            "http://vote.com/ballon_dor?candidate=ronaldo&ref=polars",
        ]
    )?;

    let out = df
        .clone()
        .lazy()
        .select([
            col("a")
                .str()
                .extract(lit(r"candidate=(\w+)"), 1)
        ])
        .collect()?;

    print_function!();
    print_data(&out);

    Ok(out)
}


// Extract all occurrences of a pattern within a string with .extract_all()
// Here, extract all numbers from a string using regex,
// using the regex pattern (\d+), which matches one or more digits.
#[print_source]
pub fn ex005() -> PolarsResult<DataFrame> {
    let df = df!(
        "foo" => &["123 bla 45 asd", "xyz 678 910t"]
    )?;

    let out = df
        .clone()
        .lazy()
        .select([
            col("foo")
                .str()
                .extract_all(lit(r"(\d+)"))
                .alias("extracted_nrs")
        ])
        .collect()?;

    print_function!();
    print_data(&out);
    Ok(out)
}


// Replace a pattern
#[print_source]
pub fn ex006() -> PolarsResult<DataFrame> {
    let df = df!(
        "id" => &[1, 2],
        "text" => &["aabc123abc", "aabc456a"]
    )?;

    let out = df
        .clone()
        .lazy()
        .with_columns([
            col("text")
                .str()
                .replace(lit("abc"), lit("ABC"), false)
                .alias("text_replace (abc)"),
            col("text")
                .str()
                .replace_all(lit("abc"), lit("ABC"), false)
                .alias("text_replace_all (abc)"),
            col("text")
                .str()
                .replace(lit("a"), lit("-"), false)
                .alias("text_replace (a)"),
            col("text")
                .str()
                .replace_all(lit("a"), lit("-"), false)
                .alias("text_replace_all (a)"),

        ])
        .collect()?;

    print_function!();
    print_data(&out);

    Ok(out)
}

//----------

fn run_all() {
    // Sample DataFrame for examples
    hr2();
    let df = ex001().unwrap();
    pause();

    // len_bytes and len_chars
    clear_screen();
    hr2();
    let result002 = ex002(&df).unwrap();
    pause();

    // String parsing
    clear_screen();
    hr2();
    let result003 = ex003(&df).unwrap();
    pause();

    // Extract a pattern
    clear_screen();
    hr2();
    let result004 = ex004().unwrap();
    pause();

    // Extract all occurrences of pattern
    clear_screen();
    hr2();
    let result005 = ex005().unwrap();
    pause();

    // Replace a pattern
    clear_screen();
    hr2();
    let result006 = ex006().unwrap();
}

fn run_individually() {
    // Sample DataFrame for examples
    hr2();
    let df = ex001().unwrap();
    pause();

    // len_bytes and len_chars
    clear_screen();
    hr2();
    let result002 = ex002(&df).unwrap();
    pause();

    // String parsing
    clear_screen();
    hr2();
    let result003 = ex003(&df).unwrap();
    pause();

    // Extract a pattern
    clear_screen();
    hr2();
    let result004 = ex004().unwrap();
    pause();

    // Extract all occurrences of pattern
    clear_screen();
    hr2();
    let result005 = ex005().unwrap();
    pause();

    // Replace a pattern
    clear_screen();
    hr2();
    let result006 = ex006().unwrap();
}

//----------

pub fn run(flag: Option<&str>) {
    println!("013 String examples");
    println!("https://docs.pola.rs/user-guide/expressions/strings/");

    if let Some(arg) = flag {
        println!("Running examples individually.\n");
        run_individually();
    } else {
        println!("Running all examples.\n");
        run_all();
    }
}
