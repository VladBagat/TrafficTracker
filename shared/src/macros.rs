#[macro_export]
macro_rules! current_time_micros {
    () => {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64
    };
}
