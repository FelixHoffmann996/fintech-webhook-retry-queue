mod webhook_retry;

use std::env;
use std::process::ExitCode;

use webhook_retry::{enqueue, run_worker, InfraiQueueClient};

fn usage() {
    eprintln!("usage:\n  webhook-retry enqueue <event-json>\n  webhook-retry worker");
}

fn main() -> ExitCode {
    let key = match env::var("INFRAI_API_KEY") {
        Ok(value) if !value.is_empty() => value,
        _ => {
            eprintln!("set INFRAI_API_KEY before running this command");
            return ExitCode::from(2);
        }
    };
    let webhook_url = match env::var("WEBHOOK_URL") {
        Ok(value) if !value.is_empty() => value,
        _ => {
            eprintln!("set WEBHOOK_URL before running this command");
            return ExitCode::from(2);
        }
    };

    let mut args = env::args().skip(1);
    let command = args.next();
    let client = InfraiQueueClient::new(key);
    let result = match command.as_deref() {
        Some("enqueue") => match args.next() {
            Some(event) if args.next().is_none() => enqueue(&client, &webhook_url, &event),
            _ => {
                usage();
                return ExitCode::from(2);
            }
        },
        Some("worker") if args.next().is_none() => run_worker(&client),
        _ => {
            usage();
            return ExitCode::from(2);
        }
    };

    match result {
        Ok(message) => {
            println!("{message}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::from(1)
        }
    }
}
