use sqlparser::dialect::Dialect;

#[derive(Debug, Default)]
pub struct URLDialect;

// 实现自己的 sql 方言, 使得支持 url
impl Dialect for URLDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        let res = ('a'..='z').contains(&ch) || ('A'..='Z').contains(&ch);

        res
    }

    // URL 中可能包含的字符, 都认为是符号的一部分
    fn is_identifier_part(&self, ch: char) -> bool {
        let res = ('a'..='z').contains(&ch)
        || ('A'..='Z').contains(&ch)
        || ('0'..='9').contains(&ch)
        || [':', '/', '?', '&', '=', '-', '_', '.'].contains(&ch);

        res
    }
}

#[cfg(test)]
mod tests {
    
    use sqlparser::parser::Parser;

    use super::URLDialect;

    #[test]
    fn it_works() {
        let url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv";

        let sql = format!(
            "SELECT location name, total_cases, new_cases, total_deaths, new_deaths \
            FROM {url} where new_deaths >= 500 ORDER BY new_cases DESC LIMIT 6 OFFSET 5"
        );
        
        let url_dialect = URLDialect::default();

        assert!(Parser::parse_sql(&url_dialect, &sql).is_ok());
    }
}