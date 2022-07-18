# sql2df
将 sql 语句转化为 DataFrame 的操作, 支持 csv, parquet.

# 构建 & 使用
- polar 的使用:
`cargo run --example pola_test`

- 

# 主要流程
1. 解析 sql 获得 ast: query(with, body(select, ), order_by, limit, offset, fetch, lock)
2. 获得 table, 读取 table 的 data, 加载为 DataFrame
3. 将 SQL ast 描述的操作转换为 DataFrame 的操作 (关键)
4. 操作 DataFrame
5. 输出