use crate::print_function;
use crate::utilities::*;
use my_proc_macro::print_source;
use polars::prelude::*;
use reqwest::blocking::Client;

//----------

// Create sample DataFrame for following examples
// download_file is a function defined in utility
#[print_source]
pub fn ex001() -> PolarsResult<DataFrame> {
    let url = "https://gist.githubusercontent.com/ritchie46/cac6b337ea52281aa23c049250a4ff03/raw/89a957ff3919d90e6ef2d34235e6bf22304f3366/pokemon.csv";
    let output_path = "./src/ex_016_aggregation_pokemon.csv";

    match download_file(url, output_path) {
        Ok(_) => println!("Process completed for file: {:?}\n", output_path),
        Err(e) => eprintln!("Error: {:?}\n", e),
    }

    let mut data: Vec<u8> = Client::new()
        .get(url)
        .send()
        .map_err(|e| PolarsError::ComputeError(format!("Failed to send request: {}", e).into()))?
        .text()
        .map_err(|e| PolarsError::ComputeError(format!("Failed to get text: {}", e).into()))?
        .bytes()
        .collect();

    let mut dataset = CsvReadOptions::default()
        .with_has_header(true)
        .with_infer_schema_length(Some(100))
        .map_parse_options(|parse_options| parse_options.with_try_parse_dates(true))
        .try_into_reader_with_file_path(Some(output_path.into()))?
        .finish()?;

    print_function!();
    print_data(&dataset);
    Ok(dataset)
}


// Group by aggregations in selection
#[print_source]
fn ex002(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .select([
            col("Type 1"),
            col("Type 2"),
            col("Attack"),
            col("Attack")
                .mean()
                .over(["Type 1"])
                .alias("avg_attack_by_type_1"),
            col("Defense"),
            col("Defense")
                .mean()
                .over(["Type 1", "Type 2"])
                .alias("avg_defense_by_type_1+2_combination"),
            col("Attack")
                .mean()
                .alias("avg_attack")
        ])
        .collect()?;

    print_function!();
    print_data(&out);
    Ok(out)
}

// Operations per group
// Create a filtered sample DataFrame for following examples
#[print_source]
fn ex003(df: &DataFrame) -> PolarsResult<DataFrame> {
    let filtered = df
        .clone()
        .lazy()
        .filter(
            col("Type 2").eq(lit("Psychic"))
        )
        .select([
            col("Name"),
            col("Type 1"),
            col("Speed")
        ])
        .collect()?;

    print_function!();
    print_data(&filtered);
    Ok(filtered)

}


// Sort column "Speed" in descending order, via window functions
#[print_source]
fn ex004(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()
        .with_columns([
            cols(["Name", "Speed"])
                .sort_by(
                    ["Speed"],
                    SortMultipleOptions::default().with_order_descending(true),
                )
            .over(["Type 1"])
        ])
        .collect()?;

    print_function!();
    print_data(&out);
    Ok(out)
}

// More examples:
// sort all pokemon by type
// select the first 3 pokemon per type as "Type 1"
// sort the pokemon within a type by speed in descending order and select the first 3 as "fastest/group"
// sort the pokemon within a type by attack in descending order and select the first 3 as "strongest/group"
// sort the pokemon within a type by name and select the first 3 as "sorted_by_alphabet"
#[print_source]
fn ex005(df: &DataFrame) -> PolarsResult<DataFrame> {
    let out = df
        .clone()
        .lazy()

        // Need this line to replicate Python example
        .sort(
            ["Type 1"],
            SortMultipleOptions::default().with_order_descending(false),
        )

        .select([

            col("Type 1")
                .head(Some(3))
                .over_with_options(
                    ["Type 1"],
                    Some((["Type 1"], SortOptions::default())),
                    WindowMapping::Explode
                )
                .flatten(),

            col("Name")
                .sort_by(
                    ["Speed"],
                    SortMultipleOptions::default().with_order_descending(true),
                )
                .head(Some(3))
                .over_with_options(
                    ["Type 1"],
                    Default::default(),
                    WindowMapping::Explode
                )
                .flatten()
                .alias("fastest/group"),

            col("Name")
                .sort_by(
                    ["Attack"],
                    SortMultipleOptions::default().with_order_descending(true),
                )
                .head(Some(3))
                .over_with_options(
                    ["Type 1"],
                    Default::default(),
                    WindowMapping::Explode
                )
                .flatten()
                .alias("strongest/group"),

            col("Name")
                .sort(
                    Default::default()
                    //SortOptions::default().with_order_descending(false),
                )
                .head(Some(3))
                .over_with_options(
                    ["Type 1"],
                    //Some((["Type 1"], SortOptions::default())),
                    Default::default(),
                    WindowMapping::Explode
                )
                .flatten()
                .alias("sorted_by_alphabet"),
        ])

        .collect()?;

    print_function!();
    print_data(&out);
    Ok(out)
}

//----------

fn run_all() {
    clear_screen();
    hr2();
    let df = ex001().unwrap();

    hr2();
    let result002 = ex002(&df).unwrap();

    hr2();
    let df_filtered = ex003(&df).unwrap();

    hr2();
    let result004 = ex004(&df_filtered).unwrap();

    hr2();
    let result005 = ex005(&df).unwrap();
}

fn run_individually() {
    clear_screen();
    hr2();
    let df = ex001().unwrap();
    pause();

    clear_screen();
    hr2();
    let result002 = ex002(&df).unwrap();
    pause();

    clear_screen();
    hr2();
    let df_filtered = ex003(&df).unwrap();
    pause();

    clear_screen();
    hr2();
    let result004 = ex004(&df_filtered).unwrap();
    pause();

    clear_screen();
    hr2();
    let result005 = ex005(&df).unwrap();
}

//----------

pub fn run(flag: Option<&str>) {
    println!("016 Window functions examples");
    println!("https://docs.pola.rs/user-guide/expressions/window//");

    if let Some(arg) = flag {
        println!("Running examples individually.\n");
        run_individually();
    } else {
        println!("Running all examples.\n");
        run_all();
    }
}
