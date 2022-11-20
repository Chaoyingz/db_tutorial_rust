use std::io;
use std::io::{Result, Write};
use std::process;

pub enum MetaCommandResult {
    MetaCommandSuccess,
    MstaCommandUnrecognizedCommand,
}

#[derive(PartialEq)]
pub enum PrepareResult {
    PrepareSuccess,
    PrepareSyntaxError,
    PrepareUnrecognizedStatment,
}

#[derive(PartialEq, Debug)]
pub enum StatementType {
    StatementInsert,
    StatementSelect,
    StatementUnsupported,
}

#[derive(Debug)]
struct Row {
    id: u32,
    username: String,
    email: String,
}

#[derive(Debug)]
struct Statement {
    stmt_type: StatementType,
    row_to_insert: Option<Row>,
}

const ID_SIZE: usize = std::mem::size_of::<u32>();
const USERNAME_SIZE: usize = 32;
const EMAIL_SIZE: usize = 255;
const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = TABLE_MAX_PAGES * ROWS_PER_PAGE;

struct Page {
    rows: Vec<Row>,
}

impl Page {
    fn new() -> Self {
        Self {
            rows: Vec::with_capacity(ROWS_PER_PAGE),
        }
    }

    unsafe fn row_slot(&self, index: usize) -> *const Row {
        self.rows.as_ptr().offset(index as isize)
    }

    unsafe fn row_mut_slot(&mut self, index: usize) -> *mut Row {
        if self.rows.capacity() <= 0 {
            self.rows.reserve(ROWS_PER_PAGE);
        }
        self.rows.as_mut_ptr().offset(index as isize)
    }
}

struct Table {
    num_rows: usize,
    pages: Vec<Page>,
}

impl Table {
    fn new() -> Self {
        Table {
            num_rows: 0,
            pages: Vec::with_capacity(TABLE_MAX_PAGES),
        }
    }

    unsafe fn page_slot(&self, index: usize) -> *const Page {
        self.pages.as_ptr().offset(index as isize)
    }

    unsafe fn page_mut_slot(&mut self, index: usize) -> *mut Page {
        self.pages.as_mut_ptr().offset(index as isize)
    }

    fn free(&mut self) {}
}

pub enum ExecuteResult {
    ExecuteSuccess,
    ExecuteFail,
    ExecuteTableFull,
}

fn print_prompt() -> Result<()> {
    print!("db > ");
    io::stdout().flush()?;
    Ok(())
}

fn read_input() -> Result<String> {
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

fn prepare_statement(command: &str) -> (Option<Statement>, PrepareResult) {
    let stmt = if command.starts_with("insert") {
        let splits: Vec<&str> = command.split(" ").collect();
        if splits.len() < 4 {
            return (None, PrepareResult::PrepareSyntaxError);
        }
        Statement {
            stmt_type: StatementType::StatementInsert,
            row_to_insert: Some(Row {
                id: splits[1].trim().parse().unwrap(),
                username: String::from(splits[2].trim()),
                email: String::from(splits[3].trim()),
            }),
        }
    } else if command.starts_with("select") {
        Statement {
            stmt_type: StatementType::StatementSelect,
            row_to_insert: None,
        }
    } else {
        Statement {
            stmt_type: StatementType::StatementUnsupported,
            row_to_insert: None,
        }
    };

    if stmt.stmt_type == StatementType::StatementUnsupported {
        (Some(stmt), PrepareResult::PrepareSyntaxError)
    } else {
        (Some(stmt), PrepareResult::PrepareSuccess)
    }
}

fn execute_statement(stmt: &Statement, table: &mut Table) -> ExecuteResult {
    match stmt.stmt_type {
        StatementType::StatementInsert => execute_insert(stmt, table),
        StatementType::StatementSelect => execute_select(stmt, table),
        _ => ExecuteResult::ExecuteFail,
    }
}

unsafe fn row_mut_slot(table: &mut Table, row_num: usize) -> *mut Row {
    let page = table.page_mut_slot(row_num / ROWS_PER_PAGE);
    (*page).row_mut_slot(row_num % ROWS_PER_PAGE)
}

unsafe fn row_slot(table: &Table, row_num: usize) -> *const Row {
    let page = table.page_slot(row_num / ROWS_PER_PAGE);
    if page.is_null() {
        return std::ptr::null();
    }
    (*page).row_slot(row_num % ROWS_PER_PAGE)
}

fn execute_insert(statement: &Statement, table: &mut Table) -> ExecuteResult {
    match &statement.row_to_insert {
        Some(row_to_insert) => {
            if table.num_rows > TABLE_MAX_ROWS {
                return ExecuteResult::ExecuteTableFull;
            }
            unsafe {
                let row = row_mut_slot(table, table.num_rows);
                std::ptr::write(
                    row,
                    Row {
                        id: (*row_to_insert).id,
                        username: String::from((*row_to_insert).username.as_str()),
                        email: String::from((*row_to_insert).email.as_str()),
                    },
                )
            }
            table.num_rows += 1;
            ExecuteResult::ExecuteSuccess
        }
        None => ExecuteResult::ExecuteFail,
    }
}

fn execute_select(statement: &Statement, table: &Table) -> ExecuteResult {
    for i in 0..table.num_rows {
        unsafe {
            let row = row_slot(table, i);
            println!("{}, {}, {}", (*row).id, (*row).username, (*row).email);
        }
    }
    ExecuteResult::ExecuteSuccess
}

fn main() -> Result<()> {
    let mut table = Table::new();
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
                }
            }
        }

        let (stmt, prepare_result) = prepare_statement(&command);
        match prepare_result {
            PrepareResult::PrepareUnrecognizedStatment => {
                println!("Unrecognized keyword at start of {}.", command);
                continue;
            }
            PrepareResult::PrepareSyntaxError => {
                println!("Syntax error. Could not parse statement.");
                continue;
            }
            _ => {}
        }
        if let Some(statement) = stmt {
            execute_statement(&statement, &mut table);
            println!("Executed.");
        }
    }
}
