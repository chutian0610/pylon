use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

/// Pylon：Arrow-native SQL 查询引擎（M1 单机）。
#[derive(Parser, Debug)]
#[command(name = "pylon", version)]
struct Cli {
    /// 要执行的 SQL（单条 SELECT）
    sql: String,

    /// 数据目录：表 t 解析为 <DIR>/t/*.parquet
    #[arg(long, env = "PYLON_DATA_DIR", default_value = ".", value_name = "DIR")]
    data_dir: PathBuf,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match pylon::run_query(&cli.sql, &cli.data_dir) {
        Ok(result) => result,
        Err(err) => {
            eprintln!("{}", err.render(&cli.sql));
            return ExitCode::from(1);
        }
    };
    match pylon::output::render_table(&result.schema, &result.batches) {
        Ok(text) => {
            let mut stdout = std::io::stdout().lock();
            if let Err(e) = writeln!(stdout, "{text}") {
                if e.kind() != std::io::ErrorKind::BrokenPipe {
                    eprintln!("error: failed to write result: {e}");
                    return ExitCode::from(2);
                }
            }
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("error: failed to render result: {err}");
            ExitCode::from(2)
        }
    }
}
