use std::io;
use std::io::{Write, Result};
use std::process;

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


fn main() -> Result<()> {
    loop {
        print_prompt()?;
        let command = read_input()?;
        if command.eq(".exit") {
            process::exit(0x0100);
        } else {
            println!("Unrecognized command {}", command);
        }
    }
}
