
use sqlparser::{dialect::GenericDialect, parser::Parser, ast::Statement};
use log::{info, warn, error};

fn main() {
    env_logger::init();
    // tracing_subscriber::fmt::init();

    // let sql = "SELECT a a1, b, 123, myfunc(b), * \
    // FROM data_source \
    // WHERE a > b AND b < 100 AND c BETWEEN 10 AND 20 \
    // ORDER BY a DESC, b \
    // LIMIT 50 OFFSET 10";
    let sql = "UPDATE xx SET a=1 WHERE b < 5";

    let ast = Parser::parse_sql(&GenericDialect::default(), sql).unwrap();
    println!("{:#?}", ast);

    let q = &ast[0];
    match q {
        Statement::Query(_) => {

        },
        _ => {
            info!("only support select!");
            error!("only support select!");
            // println!("only support select!");
        }
    }
}