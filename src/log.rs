/* logging functionality */
pub enum LogLevel {
    Debug,
    Info,
    Warning,
    Critical
}

pub fn log(verbosity: LogLevel, msg: &str) {
    let dt = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    match verbosity {
        LogLevel::Debug => {
            println!("[{}] {:>10} - {}", dt, "DEBUG" , msg);
        },
        LogLevel::Info => {
            println!("[{}] {:>10} - {}", dt, "INFO" , msg);            
        },
        LogLevel::Warning => {
            println!("[{}] {:>10} - {}", dt, "WARNING" , msg);            
        },
        LogLevel::Critical => {
            println!("[{}] {:>10} - {}", dt, "CRITICAL" , msg);
        }
    }
}