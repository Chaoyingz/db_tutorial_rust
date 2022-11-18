use std::io;
use std::io::{Write, Result};
use std::process;

pub enum MetaCommandResult {
    MetaCommandSuccess,
    MstaCommandUnrecognizedCommand,
}

#[derive(PartialEq)]
pub enum PrepareResult {
    PrepareSuccess,
    PrepareUnrecognizedStatment,
}

pub enum StatementType {
    StatementInsert,
    StatementSelect,
    StatementUnsupported,
}

struct Statement {
    stmt_type: StatementType,
}

fn print_prompt() -> Result<()> {
    print!("db > ");
    io::stdout().flush()?;
    Ok(())
}

fn read_input() -> Result<String>{
    let mut buffer = String::new();
    io::stdin().read_line(&mut buffer)?;
    if buffer.is_empty() {
        panic!("Error reading input");
    }

    Ok(buffer.trim().to_string())
}


fn do_meta_command(command: &str) -> MetaCommandResult {
    if command.eq(".exit") {
        process::exit(0x0100);
    }
    MetaCommandResult::MstaCommandUnrecognizedCommand
}

fn prepare_statement(command: &str) -> (Statement, PrepareResult) {
    let stmt_type = if command.starts_with("insert") {
        StatementType::StatementInsert
    } else if command.starts_with("select") {
        StatementType::StatementSelect
    } else {
        StatementType::StatementUnsupported
    };

    let stmt = Statement { stmt_type };

    match stmt.stmt_type {
        StatementType::StatementUnsupported => (stmt, PrepareResult::PrepareUnrecognizedStatment),
        _ => (stmt, PrepareResult::PrepareSuccess),
    }
}

fn execute_statement(stmt: &Statement) {
    match stmt.stmt_type {
        StatementType::StatementInsert => {
            println!("This is where we would do an insert");
        },
        StatementType::StatementSelect => {
            println!("This is where we would do an select");
        },
        _ => panic!("statement unsupported."),
    }
}


fn main() -> Result<()> {
    loop {
        print_prompt()?;
        let command = read_input()?;
        if command.starts_with(".") {
            let meta_result = do_meta_command(&command);
            match meta_result {
                MetaCommandResult::MetaCommandSuccess => continue,
                MetaCommandResult::MstaCommandUnrecognizedCommand => {
                    println!("Unrecognized command {}", command);
                    continue;
                },
            }
        }

        let (stmt, prepare_result) = prepare_statement(&command);
        if prepare_result == PrepareResult::PrepareUnrecognizedStatment {
            println!("Unrecognized keyword at start of {}.", command);
            continue;
        }
        execute_statement(&stmt);
        println!("Executed.");
    }
}
