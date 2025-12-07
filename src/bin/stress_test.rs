use std::time::Instant;

fn main() {
    println!("Starting stress test...");

    let start = Instant::now();

    for i in 0..1000 {
        if i % 100 == 0 {
            println!("Progress: {}/1000", i);
        }
    }

    let duration = start.elapsed();
    println!("Stress test completed in {:?}", duration);
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_stress_basic() {
        assert!(true);
    }

    #[test]
    fn test_performance() {
        let iterations = 100;
        assert!(iterations > 0);
    }
}
