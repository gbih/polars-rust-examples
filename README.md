

# Source Material

* Annotated and modified source code from the Rust version of the [Polars user guide](https://docs.pola.rs/).

* I have modified this source code for learning purposes, mainly for more explicitness and/or exploring further variations on a given topic.


# Usage

* Each script corresponds to a section in the Polars user guide:

1: Getting Started
2: Categorical
3: Enum
4: Data Structures
5: Contexts
6: Expressions
7: Lazy API
8: Streaming
9: Expression Operators
10: Expression Column Selections
11: Functions
12: Casting
13: Strings
14: Aggregation 
15: Missing data
16: Aggregation window
17: Folds
18: List arrays
19: Struct


For example, to run the Categorical examples listed in https://docs.pola.rs/user-guide/concepts/data-types/categoricals/#categorical-data-type, use 

```
cargo run -- -n 2
```

---

# Installation Notes

* reqwest needs to be 11.27 to be compatible


Cargo.toml configuration:

````
[package]
name = "polars_rust_examples"
version = "0.1.0"
edition = "2021"

[dependencies]
my_proc_macro = { path = "./my_proc_macro" }
quote = "1.0.37"
polars = { version = "0.42.0", features = [
	"lazy",
	"dtype-categorical",
	"streaming",
	"regex",
	"strings",
	"interpolate",
	"concat_str",
	"list_eval",
	"rank",
	"round_series",
	"dtype-struct"
]}

indoc = "2.0.5"
chrono = "0.4.38"
clap = { version = "4.5.17" }
polars-arrow = "0.42.0"
rand = "0.8.5"
reqwest = { version = "0.11.27", features = ["blocking"] }
```
