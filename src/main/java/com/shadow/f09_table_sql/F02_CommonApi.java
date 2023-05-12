package com.shadow.f09_table_sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 2、Flink Table & SQL 基础使用 {@see https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/overview/}
 * 1）TableEnvironment#executeSql() 用于执行 DDL/DML/DQL/SHOW/DESCRIBE/EXPLAIN/USE
 * 2）TableEnvironment#sqlQuery() 用于执行 SQL
 * 3）TableEnvironment#createTemporaryView() 用于创建临时表，创建后可以在SQL中直接使用
 * 4）TableEnvironment#from(tableName) 用于创建Table对象
 * <p>
 * Table API：
 * 1）Table#select() 查询指定的列
 * 2）Table#where() 查询条件
 * 3）Table#as() 给列取别名
 * 4）Table#groupBy() 分组
 * 5）Table#orderBy() 排序
 * 6）Table#limit() 分页
 * 7）Table#limit() 分页
 * 8）Table#join() 内连接
 * 9）Table#leftOuterJoin() 左外连接
 * ...
 */
public class F02_CommonApi {
    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 2. 创建表
        String createDDL = "CREATE TABLE clickTable (" +
                " user_name STRING, " +
                " url STRING, " +
                " ts BIGINT " +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'input/clicks.csv', " +
                " 'format' =  'csv' " +
                ")";
        tableEnv.executeSql(createDDL);

        // 3. 表的查询转换
        // 3.1 调用Table API
        Table clickTable = tableEnv.from("clickTable");
        Table resultTable = clickTable.where($("user_name").isEqual("Bob"))
                .select($("user_name"), $("url"));

        tableEnv.createTemporaryView("resultTable", resultTable);

        // 3.2 执行SQL进行表的查询转换
        Table resultTable2 = tableEnv.sqlQuery("select url, user_name from resultTable");

        // 3.3 执行聚合计算的查询转换
        Table aggResult = tableEnv.sqlQuery("select user_name, COUNT(url) as cnt from clickTable group by user_name");

        // 4. 创建一张用于输出的表
        String createOutDDL = "CREATE TABLE outTable (" +
                " url STRING, " +
                " user_name STRING " +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'output', " +
                " 'format' =  'csv' " +
                ")";

        tableEnv.executeSql(createOutDDL);

        // 创建一张用于控制台打印输出的表
        String createPrintOutDDL = "CREATE TABLE printOutTable (" +
                " user_name STRING, " +
                " cnt BIGINT " +
                ") WITH (" +
                " 'connector' = 'print' " +
                ")";

        tableEnv.executeSql(createPrintOutDDL);

        // 5. 输出表
//        resultTable.executeInsert("outTable");
//        resultTable.executeInsert("printOutTable");
        aggResult.executeInsert("printOutTable");
    }
}
