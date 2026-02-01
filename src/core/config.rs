#[derive(Debug, Clone)]
pub struct Config {
    pub base_path: String,
    pub max_memory_chunk_rows: usize,
    pub max_memory_chunk_age_seconds: u64,
    pub http_port: u16,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            base_path: "./data".to_string(),
            max_memory_chunk_rows: 10000,
            max_memory_chunk_age_seconds: 60,
            http_port: 8080,
        }
    }
}
