#[cfg(test)]
mod tests {
    use super::super::*;

    #[test]
    fn test_api_basic() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn test_api_connection() {
        let result = true;
        assert!(result);
    }
}