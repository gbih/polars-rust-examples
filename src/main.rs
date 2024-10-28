#![allow(unused)]
mod utilities;
pub use utilities::*;

use clap::{Arg, ArgAction, Command};

//--------------------

mod ex_001_getting_started;
mod ex_002_categorical;
mod ex_003_enum;
mod ex_004_data_structures;
mod ex_005_contexts;
mod ex_006_expressions;
mod ex_007_lazy_eager_api;
mod ex_008_streaming_api;
mod ex_009_expression_operators;
mod ex_010_expression_column_selections;
mod ex_011_functions;
mod ex_012_casting;
mod ex_013_strings;
mod ex_014_aggregation;
mod ex_015_missing_data;
mod ex_016_window;
mod ex_017_folds;
mod ex_018_lists_arrays;
mod ex_019_struct;

//--------------------

fn help_message() {
    println!("Example: cargo run -- -n 1");
}

//--------------------


fn run() {
    let matches = Command::new("input")
        .version("0.1.0")
        .about("Rust Polars Examples")
        .arg(
            Arg::new("numeric_option")
                .short('n')
                .value_name("NUMBER")
                .long("number")
                //.default_value("1")
                .help("Example: cargo run -- -n 1"),
        )
        .arg(
            Arg::new("flag")
                .short('s')
                .value_name("STRING")
                .long("string")
                .help("Example: cargo run -- -s p")
        )
        .get_matches();

    let flag = matches
        .get_one::<String>("flag")
        .map(|s| s.as_str());

    match matches.get_one::<String>("numeric_option") {
        Some(number_str) => match number_str.parse::<i32>() {
            Ok(0) => check_threads_n(),
            Ok(1) => ex_001_getting_started::run(flag),
            Ok(2) => ex_002_categorical::run(flag),
            Ok(3) => ex_003_enum::run(flag),
            Ok(4) => ex_004_data_structures::run(flag),
            Ok(5) => ex_005_contexts::run(flag),
            Ok(6) => ex_006_expressions::run(flag),
            Ok(7) => ex_007_lazy_eager_api::run(flag),
            Ok(8) => ex_008_streaming_api::run(flag),
            Ok(9) => ex_009_expression_operators::run(flag),
            Ok(10) => ex_010_expression_column_selections::run(flag),
            Ok(11) => ex_011_functions::run(flag),
            Ok(12) => ex_012_casting::run(flag),
            Ok(13) => ex_013_strings::run(flag),
            Ok(14) => ex_014_aggregation::run(flag),
            Ok(15) => ex_015_missing_data::run(flag),
            Ok(16) => ex_016_window::run(flag),
            Ok(17) => ex_017_folds::run(flag),
            Ok(18) => ex_018_lists_arrays::run(flag),
            Ok(19) => ex_019_struct::run(flag),

            //-----
            Ok(_) | Err(_) => help_message(),
        },
        None => help_message(),
    }
}

//--------------------

fn main() {
    //clear_screen();
    hr1();
    run();
}
