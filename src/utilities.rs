use std::io::{stdin, stdout, Read, Write};
use polars::frame::DataFrame;
use std::thread;

// Utilities
#[macro_export]
macro_rules! print_function {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);
        let name_out = &name[(0..name.len() - 3)];
        println!("FUNCTION  : {}", &name[(0..name.len() - 3)]);
    }};
}



pub fn hr1() {
    println!("{}", "=".repeat(80));
}

pub fn hr2() {
    println!("{}", "*".repeat(70));
}

pub fn hr3() {
    println!("{}", "-".repeat(60));
}

pub fn print_type<T>(_: &T) {
    println!("DATA TYPE : {}", std::any::type_name::<T>());
}

pub fn clear_screen() {
    // \x1B[2J: Clears the entire screen. \x1B is the escape character (ASCII 27), and [2J is the command to clear the screen
    // \x1B[1;1H: Moves the cursor to the top-left corner of the screen (row 1, column 1).
    println!("\x1B[2J\x1B[1;1H");
}

pub fn pause() {
    let mut stdout = stdout();
    stdout.write(b"\n[ Press Enter to continue ] ").unwrap();
    stdout.flush().unwrap();
    stdin().read(&mut [0]).unwrap();
}

pub fn print_data(input: &DataFrame) {
    print_type(&input);
    println!("DATA      : {:?}", &input);
}


// utility function
// Need to enable the "blocking" feature for the reqwest crate
use reqwest::blocking::Client;
use std::fs::File;
use std::io::copy;
use std::path::Path;

pub fn download_file(url: &str, output_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    if Path::new(output_path).exists() {
        println!("File already exists at: {:?}", output_path);
        return Ok(());
    }

    // Create a new blocking client
    // Send a GET request to the URL
    // Create the output file
    // Copy the response body to the output file
    let client = Client::new()
        .get(url)
        .send()?
        .copy_to(&mut File::create(output_path)?)?;

    println!("File downloaded successfully to: {:?}", output_path);
    Ok(())
}

// Detect the number of logical CPUs available on the system
pub fn check_threads_n() {
    match thread::available_parallelism() {
        Ok(num) => println!("Number of logical CPUs: {}", num),
        Err(e) => println!("Couldn't get number of CPUs: {}", e),
    }
}
