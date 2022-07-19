# sql2df
将 sql 语句转化为 DataFrame 的操作, 目前支持 csv 文件的加载.

# 构建 & 使用
- polar 的使用
`cargo run --example pola_test`

- 示例程序
`cargo run --example covid`

# 主要流程
1. 解析 sql 获得 ast, ast 格式参考: docs/sql_ast_example.json
2. 将 SQL ast 描述的操作转换为 DataFrame 的操作
3. 获取 source 指定的数据源, 加载为 DataFrame
4. 操作 DataFrame
5. 输出