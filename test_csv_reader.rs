fn main() {
    println\!("Testing LazyCsvReader...");
    
    // Create a test CSV file
    let test_data = "col1,col2,col3
1,2,3
4,5,6
7,8,9
";
    std::fs::write("test.csv", test_data).unwrap();
    
    // Test with LazyCsvReader
    use polars::prelude::*;
    let start = std::time::Instant::now();
    let df = LazyCsvReader::new("test.csv")
        .with_has_header(true)
        .finish()
        .unwrap()
        .collect()
        .unwrap();
    
    println\!("LazyCsvReader time: {:?}", start.elapsed());
    println\!("DataFrame shape: {:?}", df.shape());
    println\!("DataFrame:
{}", df);
    
    // Clean up
    std::fs::remove_file("test.csv").unwrap();
}
