use polars::prelude::*;
use polars::datatypes::CategoricalOrdering::*;
use polars_arrow::array::*;

use crate::utilities::*;
use crate::print_function;

//-----


// https://github.com/pola-rs/polars/issues/14084#issuecomment-2049697806
// Create enum-backed DataFrame
pub fn ex001() -> PolarsResult<DataFrame> {
    print_function!();

    // 1. Create enum
    let vec = vec![
        Some("January"),
        Some("February"),
        Some("March"),
        Some("April"),
        Some("May"),
        Some("June"),
        Some("July"),
        Some("August"),
        Some("September"),
        Some("October"),
        Some("November"),
        Some("December")
    ];


    print_type(&vec); // Vec<Option<&str>>
    println!("vec ::: {:?}", vec);
    hr3();

    // BinaryViewArrayGeneric: specialized array type
    // Often used for categorical data and enumerations, in structures like RevMapping
    // Utf8ViewArray[December, January, December, May, March]
    // Utf8ViewArray is part of the polars::datatypes module and corresponds to the ArrowDataType::Utf8View data type.
    /* Utf8View is part of the Arrow data format, which is widely used in data processing libraries
    and frameworks. The Polars library uses this type when interfacing with Arrow
    data structures, providing efficient handling of string data in data processing pipelines. */

    /// Create polars_arrow data structure: polars_arrow::array::BinaryViewArrayGeneric<str>
    // categories: Utf8ViewArray
    let categories = BinaryViewArrayGeneric::<str>::from_slice(&vec);
    print_type(&categories); // BinaryViewArrayGeneric<str>
    println!("categories ::: {:?}", categories);
    hr3();

    // thread-safe reference-counted pointer to a RevMapping structure used in categorical data handling
    // This is needed to compose df.lazy().cast(DataType::Enum(Option<Arc<RevMapping>>, CategoricalOrdering))
    // pub fn build_local(categories: Utf8ViewArray) -> Self
    let arc_rev_mapping = Arc::new(RevMapping::build_local(categories));
    print_type(&arc_rev_mapping); // Arc<RevMapping>
    println!("arc_rev_mapping ::: {:?}", arc_rev_mapping);
    hr3();


    // 2. Create DataFrame and cast to this enum type
    let series = Series::new("items", &["December", "February", "March", "January", "November", "October", "December"]);
    print_type(&series); // Series
    println!("series ::: {:?}", series);
    hr3();

    // let df = DataFrame::new(vec![series]).unwrap();
    // print_type(&df); // DataFrame
    // println!("df ::: {:?}", df);
    // hr();

    // pseudo signature
    // df.lazy().cast(DataType::Enum(Option<Arc<RevMapping>>, CategoricalOrdering))
    //.unwrap().lazy()
    // let df = df.lazy()
    let df = DataFrame::new(vec![series])
            .unwrap()
            .lazy()
            .select([
                col("items")
                // Enum(Option<Arc<RevMapping>>, CategoricalOrdering)
                .cast(DataType::Enum(
                    Some(arc_rev_mapping), // expect: struct `std::sync::Arc<polars::prelude::RevMapping>`
                    CategoricalOrdering::Physical
                ))
            ])
            .sort(["items"], Default::default())
            .collect()
            .unwrap();
    print_type(&df); // DataFrame
    println!("df sorted via enum ::: {:?}", df);
    hr3();

    let d_repr = df.column("items")?.to_physical_repr();
    print_type(&d_repr);
    println!("d_repr ::: {:?}", d_repr);

    Ok(df)
}







// https://github.com/pola-rs/polars/issues/14084#issuecomment-2058929518
// Create enum-backed Series
pub fn ex002() -> PolarsResult<Series>  {
    print_function!();

    let enum_data = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
    // let enum_data: [&str; 4]
    print_type(&enum_data);
    println!("enum_data ::: {:?}", enum_data);
    hr3();


    /*
    Equivalent to: let vec = vec![Some("January"),Some("February")]
    Convert a collection of values into a collection of Option values.
    This is a common pattern in Polars when working with nullable data.

    creates an iterator over the elements in months_data.
    applies the Some function to each element of the iterator. Some is a variant of the Option enum in Rust, which is used to represent optional values.
    collects the mapped iterator into a new Vec. The <_> syntax tells Rust to infer the type of the vector elements.
    */
    let enum_vec = enum_data.iter().map(Some).collect::<Vec<_>>();
    let enum_vec2: Vec<Option<&&str>> = enum_data.iter().map(Some).collect();
    assert!(enum_vec == enum_vec2);
    print_type(&enum_vec);
    println!("enum_vec ::: {:?}", enum_vec);
    hr3();


    /*
    While BinaryViewArrayGeneric::<str> is more generic and can be used for UTF-8 strings,
    it's typically reserved for cases where you need to work with binary data that may not
    necessarily be UTF-8 encoded. In summary, unless you have a specific reason to use the more
    generic BinaryViewArrayGeneric, it's more idiomatic in Rust Polars to use Utf8ViewArray::from_slice(&s)
    when working with UTF-8 string data

    Key points about Utf8ViewArray:
    - Efficient String Storage: Utf8ViewArray is designed to store UTF-8 encoded strings in a memory-efficient manner.
    - Zero-Copy Views: It allows for creating zero-copy views of string data, which means it can reference existing string data without making additional copies.
    - Part of Arrow Memory Model: Utf8ViewArray is based on Apache Arrow's memory model, which Polars uses for its underlying data structures.
    - Used in Categorical and Enum Types: In Polars, Utf8ViewArray is used internally for representing categorical and enum data types.
    - Performance Optimization: By using Utf8ViewArray, Polars can perform certain string operations more efficiently, especially when dealing with large datasets.
    - Integration with AnyValue: The AnyValue enum in Polars, which represents various data types, includes variants that use Utf8ViewArray for string-related data.
    - Thread-Safe Access: Utf8ViewArray is typically accessed through a SyncPtr, which provides thread-safe access to the underlying data.

    While Utf8ViewArray is an important internal component of Polars, as a user of the library, you generally won't interact with it directly.
    Instead, you'll work with higher-level abstractions like Series and DataFrame that utilize these efficient data structures behind the scenes
     */

     // more idiomatic
     // creates BinaryViewArrayGeneric<str>
     // The slice is AsRef<[Option<S>]
     let categories = Utf8ViewArray::from_slice(enum_vec); // from polars_arrow crate
     // let categories: BinaryViewArrayGeneric<str>
     print_type(&categories);
     println!("categories ::: {:?}", categories);
     hr3();


    // less idiomatic
    // let months_enum = BinaryViewArrayGeneric::<str>::from_slice(s);

    // pub fn build_local(categories: Utf8ViewArray)
    let rev_mapping = RevMapping::build_local(categories); // more idiomatic
    // let rev_mapping = RevMapping::build_local(months_enum); // less common


    // RevMapping::build_local(), function to create a local reverse mapping for categorical data.
    // let rev_mapping: RevMapping
    print_type(&rev_mapping);
    println!("rev_mapping ::: {:?}", rev_mapping);
    hr3();


    let arc_rev_mapping = Arc::new(rev_mapping);
    println!("arc_rev_mapping ::: {:?}", arc_rev_mapping);
    hr3();


    // TYPE: polars_core::series::Series
    let data = Series::new("items", &["December", "February", "March", "January", "November", "October", "December"]);
    print_type(&data);
    println!("data ::: {:?}", data);
    hr3();


    /*
    Some(std::sync::Arc<polars::prelude::RevMapping>)
    This call creates a RevMapping instance and then immediately calls .into() on it.
    The .into() method is part of Rust's type conversion system and is used to convert one type into another.
    This creates the RevMapping and then converts it into another type that can use this mapping.
    This ensures that the created RevMapping is actually used in constructing or updating a categorical column or related data structure.

    The .into() method is used for type conversion. It's part of the Into trait, which is closely related to the From trait.
    The .into() method converts a value from one type to another, consuming the original value in the process.
    It's a flexible and powerful way to perform type conversions without explicitly specifying the target type.
    If a type implements the From trait for a conversion, the Into trait is automatically implemented for the reverse conversion.
    */
    // TYPE: polars_core::series::Series


    let series = data
                .cast(
                        &DataType::Enum(
                            Some(arc_rev_mapping),
                            CategoricalOrdering::Physical,
                    )
                )
                .unwrap()
                .sort(Default::default())
                .unwrap();

    print_type(&series);
    println!("series ::: {:?}", series);
    hr3();

    let series_repr = series.to_physical_repr();
    // let series_repr: Cow<'_, Series>
    // TYPE: alloc::borrow::Cow<polars_core::series::Series>
    print_type(&series_repr);
    println!("series_repr ::: {:?}", series_repr);

    Ok(series)
}






// https://docs.pola.rs/user-guide/concepts/data-types/categoricals/#using-the-global-string-cache
// Create enum-backed Series
pub fn ex003() -> PolarsResult<Series>  {
    print_function!();

    let enum_data = ["Polar", "Panda", "Brown"];
    print_type(&enum_data);
    println!("enum_data ::: {:?}", enum_data);
    hr3();

    let enum_vec = enum_data.iter().map(Some).collect::<Vec<_>>();
    let enum_vec2: Vec<Option<&&str>> = enum_data.iter().map(Some).collect();
    assert!(enum_vec == enum_vec2);
    print_type(&enum_vec);
    println!("enum_vec ::: {:?}", enum_vec);
    hr3();

    let categories = Utf8ViewArray::from_slice(enum_vec); // from polars_arrow crate
    print_type(&categories);
    println!("categories ::: {:?}", categories);
    hr3();


    // pub fn build_local(categories: Utf8ViewArray)
    let rev_mapping = RevMapping::build_local(categories); // more idiomatic
    // let rev_mapping = RevMapping::build_local(months_enum); // less common


    // RevMapping::build_local(), function to create a local reverse mapping for categorical data.
    // let rev_mapping: RevMapping
    print_type(&rev_mapping);
    println!("rev_mapping ::: {:?}", rev_mapping);
    hr3();


    let arc_rev_mapping = Arc::new(rev_mapping);
    println!("arc_rev_mapping ::: {:?}", arc_rev_mapping);
    hr3();


    // TYPE: polars_core::series::Series
    let data1 = Series::new("items1", &["Polar", "Panda", "Brown", "Brown", "Polar"]);
    print_type(&data1);
    println!("data1 ::: {:?}", data1);
    hr3();

    let data2 = Series::new("items2", &["Black", "Panda", "Brown", "Brown", "Polar", "Polar"]);
    print_type(&data2);
    println!("data2 ::: {:?}", data2);
    hr3();

    let mut series1 = data1
                .cast(
                        &DataType::Enum(
                            Some(arc_rev_mapping.clone()),
                            CategoricalOrdering::Physical,
                    )
                )
                .unwrap()
                .sort(Default::default())
                .unwrap();

    let series2 = data2
                .cast(
                        &DataType::Enum(
                            Some(arc_rev_mapping.clone()),
                            CategoricalOrdering::Physical,
                    )
                )
                .unwrap()
                .sort(Default::default())
                .unwrap();



    print_type(&series1);
    println!("series1 ::: {:?}", series1);
    hr3();

    print_type(&series2);
    println!("series2 ::: {:?}", series2);
    hr3();

    // Enum type comparisons are valid if they have the same categories.
    println!("{:?}", series1 == series2);
    // assert!(series1 == series2);


    let out = series1.append(&series2)?.clone();
    print_type(&out);
    println!("out ::: {:?}", out);

    Ok(out)
}


//-----

pub fn run(flag: Option<&str>) {
    println!("003 Enum examples");
    println!("https://docs.pola.rs/user-guide/concepts/data-types/categoricals/#enum-data-type");

    let result001 = ex001().unwrap();
    hr2();
    pause();

    let result002 = ex002().unwrap();
    hr2();
    pause();

    let result003 = ex003().unwrap();
}
