use std::convert::TryFrom;

use polars::prelude::*;
use polars_lazy::dsl::Expr as polaExpr;
use anyhow::{anyhow, Result};
use sqlparser::ast::{Statement, Expr as SqlExpr, SetExpr, Select, Offset, TableWithJoins, OrderByExpr, SelectItem, TableFactor, Value as SqlValue, BinaryOperator as SqlBinaryOperator};

// SQL 操作, SQL ast 可以参考 sql_parse.json 文件
// 目前我们仅支持如下这些操作
struct SQLOperation {
    selection: Vec<SelectItem>,  // 列选择
    condition: Option<SqlExpr>,  // 行过滤条件
    source: Vec<TableWithJoins>, // 数据源
    order_by: Vec<OrderByExpr>,
    offset: Option<Offset>,
    limit: Option<SqlExpr>
}

// polars 的 DataFrame 操作
struct DataFrameOperation {
    selection: Vec<polaExpr>,    // 列选择
    condition: Option<polaExpr>, // 行过滤条件
    source: String,          // 数据源
    order_by: Vec<(String, bool)>,
    offset: Option<i64>,
    limit: Option<usize>
}

// 使用包装规避孤儿原则
struct SelectItemWrapper(SelectItem);
struct SqlExprWrapper (SqlExpr);
struct TableWithJoinsWrapper (Vec<TableWithJoins>);
struct OrderByExprWrapper (OrderByExpr);
struct OffsetWrapper (Offset);
struct Operation(SqlBinaryOperator);
struct Value(SqlValue);

// Statement -> SQLOperation
impl TryFrom<Statement> for SQLOperation {
    type Error = anyhow::Error;

    fn try_from(sql: Statement) -> Result<Self, Self::Error> {
        match sql {
            Statement::Query(q) => {
                let Select {
                    from: source,
                    selection: condition,
                    projection: selection,
                    ..
                } = 
                match q.body {
                    SetExpr::Select(select) => {
                        *select.clone()
                    },
                    _=> {
                        return Err(anyhow!("only support select query!"));
                    }
                };
                let order_by = q.order_by;
                let offset = q.offset;
                let limit = q.limit;
                Ok(SQLOperation {
                    selection,
                    condition,
                    source,
                    order_by,
                    offset,
                    limit
                })
            },
            _ => Err(anyhow!("only support query!"))
        }
    }
}

impl TryFrom<SQLOperation> for DataFrameOperation {
    type Error = anyhow::Error;
    fn try_from(sqlop: SQLOperation) -> Result<Self, Self::Error> {
        // selection
        let mut selection: Vec<polaExpr> = Vec::new();
        for s in &sqlop.selection {
            let ps = SelectItemWrapper(s.clone()).try_into()?;
            selection.push(ps);
        }

        // condition
        let condition = match &sqlop.condition {
            Some(sqlexpr) => Some(SqlExprWrapper(sqlexpr.clone()).try_into()?),
            None => None
        };

        // source
        let source = TableWithJoinsWrapper(sqlop.source.clone()).try_into()?;

        // order_by
        let mut order_by = Vec::new();
        for o in &sqlop.order_by {
            order_by.push(OrderByExprWrapper(o.clone()).try_into()?);
        }
        // offset
        let offset = match &sqlop.offset {
            Some(o) => Some(OffsetWrapper(o.clone()).try_into()?),
            None => None
        };

        // limit
        let limit = match &sqlop.limit {
            Some(l) => Some(SqlExprWrapper(l.clone()).try_into()?),
            None => None
        };
        Ok(DataFrameOperation {
            selection,
            condition,
            source,
            order_by,
            offset,
            limit,
        })
    }
}

// selection
impl TryFrom<SelectItemWrapper> for polaExpr {
    type Error = anyhow::Error;
    fn try_from(s: SelectItemWrapper) -> Result<Self, Self::Error> {
        match s.0 {
            SelectItem::UnnamedExpr(SqlExpr::Identifier(id)) => Ok(col(&id.to_string())),
            SelectItem::ExprWithAlias {
                expr: SqlExpr::Identifier(id),
                alias,
            } => Ok(polaExpr::Alias(
                Box::new(polaExpr::Column(Arc::from(&*(id.to_string())))),
                Arc::from(&*(alias.to_string())),
            )),
            SelectItem::QualifiedWildcard(v) => Ok(col(&v.to_string())),
            SelectItem::Wildcard => Ok(col("*")),
            item => Err(anyhow!("projection {} not supported", item)),
        }
    }
}

// condition
impl TryFrom<SqlExprWrapper> for polaExpr {
    type Error = anyhow::Error;
    fn try_from(expr: SqlExprWrapper) -> Result<Self, Self::Error> {
        match expr.0 {
            SqlExpr::BinaryOp { left, op, right } => Ok(Expr::BinaryExpr {
                left: Box::new(SqlExprWrapper(*left.clone()).try_into()?),
                op: Operation(op).try_into()?,
                right: Box::new(SqlExprWrapper(*right.clone()).try_into()?),
            }),
            // SqlExpr::Wildcard => Ok(Self::Wildcard),
            SqlExpr::IsNull(expr) => Ok(Self::IsNull(Box::new(SqlExprWrapper(*expr.clone()).try_into()?))),
            SqlExpr::IsNotNull(expr) => Ok(Self::IsNotNull(Box::new(SqlExprWrapper(*expr.clone()).try_into()?))),
            SqlExpr::Identifier(id) => Ok(Self::Column(Arc::from(&*id.value))),
            SqlExpr::Value(v) => Ok(Self::Literal(Value(v).try_into()?)),
            v => Err(anyhow!("expr {:#?} is not supported", v)),
        }
    }
}

// source
impl TryFrom<TableWithJoinsWrapper> for String {
    type Error = anyhow::Error;
    fn try_from(source: TableWithJoinsWrapper) -> Result<Self, Self::Error> {
        if source.0.len() != 1 {
            return Err(anyhow!("We only support single data source at the moment"));
        }

        let table = &source.0[0];
        if !table.joins.is_empty() {
            return Err(anyhow!("We do not support joint data source at the moment"));
        }

        match &table.relation {
            TableFactor::Table { name, .. } => Ok(name.0.first().unwrap().value.clone()),
            _ => Err(anyhow!("We only support table")),
        }
    }
}

// order by
impl TryFrom<OrderByExprWrapper> for (String, bool) {
    type Error = anyhow::Error;
    fn try_from(o: OrderByExprWrapper) -> Result<Self, Self::Error> {
        let name = match &o.0.expr {
            SqlExpr::Identifier(id) => id.to_string(),
            expr => {
                return Err(anyhow!(
                    "We only support identifier for order by, got {}",
                    expr
                ))
            }
        };

        Ok((name, !o.0.asc.unwrap_or(true)))
    }
}

// offset
impl From<OffsetWrapper> for i64 {
    fn from(offset: OffsetWrapper) -> Self {
        match offset.0 {
            Offset {
                value: SqlExpr::Value(SqlValue::Number(v, _b)),
                ..
            } => v.parse().unwrap_or(0),
            _ => 0,
        }
    }
}

// limit
impl From<SqlExprWrapper> for usize {
    fn from(s: SqlExprWrapper) -> Self {
        match s.0 {
            SqlExpr::Value(SqlValue::Number(v, _b)) => v.parse().unwrap_or(usize::MAX),
            _ => usize::MAX,
        }
    }
}

impl TryFrom<Operation> for Operator {
    type Error = anyhow::Error;

    fn try_from(op: Operation) -> Result<Self, Self::Error> {
        match op.0 {
            SqlBinaryOperator::Plus => Ok(Self::Plus),
            SqlBinaryOperator::Minus => Ok(Self::Minus),
            SqlBinaryOperator::Multiply => Ok(Self::Multiply),
            SqlBinaryOperator::Divide => Ok(Self::Divide),
            SqlBinaryOperator::Modulo => Ok(Self::Modulus),
            SqlBinaryOperator::Gt => Ok(Self::Gt),
            SqlBinaryOperator::Lt => Ok(Self::Lt),
            SqlBinaryOperator::GtEq => Ok(Self::GtEq),
            SqlBinaryOperator::LtEq => Ok(Self::LtEq),
            SqlBinaryOperator::Eq => Ok(Self::Eq),
            SqlBinaryOperator::NotEq => Ok(Self::NotEq),
            SqlBinaryOperator::And => Ok(Self::And),
            SqlBinaryOperator::Or => Ok(Self::Or),
            v => Err(anyhow!("Operator {} is not supported", v)),
        }
    }
}

impl TryFrom<Value> for LiteralValue {
    type Error = anyhow::Error;
    fn try_from(v: Value) -> Result<Self, Self::Error> {
        match v.0 {
            SqlValue::Number(v, _) => Ok(LiteralValue::Float64(v.parse().unwrap())),
            SqlValue::Boolean(v) => Ok(LiteralValue::Boolean(v)),
            SqlValue::Null => Ok(LiteralValue::Null),
            v => Err(anyhow!("Value {} is not supported", v)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::URLDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn statement_2_sqloperation() {
        let url = "http://abc.xyz/abc?a=1&b=2";
        let sql = format!(
            "select a, b, c from {url} where a=1 order by c desc limit 5 offset 10"
        );
        let statement = Parser::parse_sql(&URLDialect::default(), sql.as_ref()).unwrap()[0].clone();
        let res :Result<SQLOperation, _> = statement.try_into();
        assert!(res.is_ok());
    }

    #[test]
    fn parse_sql_works() {
        let url = "http://abc.xyz/abc?a=1&b=2";
        let sql = format!(
            "select a, b, c from {url} where a=1 order by c desc limit 5 offset 10"
        );
        let statement = Parser::parse_sql(&URLDialect::default(), sql.as_ref()).unwrap()[0].clone();
        let sqlop: SQLOperation = statement.try_into().unwrap();
        let dfop: DataFrameOperation = sqlop.try_into().unwrap();
        assert_eq!(dfop.source, url);
        assert_eq!(dfop.limit, Some(5));
        assert_eq!(dfop.offset, Some(10));
        assert_eq!(dfop.order_by, vec![("c".into(), true)]);
        assert_eq!(dfop.selection, vec![col("a"), col("b"), col("c")]);
    }
}
