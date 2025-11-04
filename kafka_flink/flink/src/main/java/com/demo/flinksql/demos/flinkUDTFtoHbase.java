package com.demo.flinksql.demos;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.metrics.Counter;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Get;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.nio.charset.StandardCharsets;

/**
 * 根据 rowkey 查询hbase表数据
 */
@FunctionHint(output = @DataTypeHint("ROW<propKey STRING, gs_key STRING, gs_data STRING>"))
public class flinkUDTFtoHbase extends TableFunction<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(flinkUDTFtoHbase.class);
    private transient Connection connection;
    private transient Counter hbaseInputRecords;
    private transient Counter hbaseOutputRecords;

    /**
     *  模拟配置中心
     *  根据传入的配置文件名来动态获取hbase中要读的表以及列族和列的信息组成一个tuple3
     *  key 配置名词
     *  tuple3 f0 要关联的hbase表名
     *  f1 列族
     *  f2 具体的返回字段（顺序）
     */
    private final Map<String, Tuple3<String, String, String>> props = new HashMap<>();

    @Override
    public void open(FunctionContext context) throws Exception {
        LOG.info("initializing HBase connection...");
        if (null == connection) {
            this.connection = ConnectionFactory.createConnection(getHBaseConf());
        }
        LOG.info("HBase connection {} initialized.", connection);
        props.put("order_trace_props",Tuple3.of("order_trace","cf","express_code,to_province,to_city,to_country,to_contact_name"));

        /*
         * 自定义metrics ，技术输入和输出条数
         */
        hbaseInputRecords = context.getMetricGroup().addGroup("gs_hbase_records").counter("hbaseInputRecords");
        hbaseOutputRecords = context.getMetricGroup().addGroup("gs_hbase_records").counter("hbaseOutputRecords");
    }
    /**
     * 根据属性名称、键和值评估并从HBase中获取数据
     * 此方法的目的是根据提供的属性名称、键和值来查询HBase数据库，并收集特定列族和列的数据
     *
     * @param propName 属性名称，用于从属性集合中获取HBase连接配置
     * @param key 用于收集数据的键，代表HBase中的行键
     * @param value 查询HBase时使用的实际行键值
     */
    public void eval(String propName, String key, String value) {

        // 增加输入记录计数
        hbaseInputRecords.inc();

        // 根据属性名称获取HBase连接配置（表名、列族、列）
        Tuple3<String, String, String> conf = props.get(propName);

        // 如果配置存在，则继续处理
        if (null != conf) {
            // 解析表名 将配置中的表名转换为TableName对象
            TableName tableName = TableName.valueOf(conf.f0);

            try {
                // 获取HBase管理员实例
                Admin admin = connection.getAdmin();

                // 检查表是否存在
                if (admin.tableExists(tableName)) {
                    // 获取表实例
                    Table table = connection.getTable(tableName);

                    // 根据rowkey 创建Get实例，用于查询
                    Get get = new Get(value.getBytes(StandardCharsets.UTF_8));

                    // 获取列族和列名数组
                    String cf = conf.f1;
                    String[] cols = conf.f2.split(",");

                    // 为Get实例添加要查询的列
                    for (String col : cols) {
                        get.addColumn(cf.getBytes(StandardCharsets.UTF_8), col.getBytes(StandardCharsets.UTF_8));
                    }

                    // 执行查询并获取结果，得到的是该列族下指定的所有列的原始数据
                    Cell[] cells = table.get(get).rawCells();
                    StringBuilder data = new StringBuilder();

                    /*
                     * 需要按照顺序拼接字符串返回，最终返回的结果是根据指定列的顺序进行值的拼接，并以\u0001作为分隔符
                     */
                    for (String col : cols) {
                        for (Cell cell : cells) {
                            // 获取单元格列名
                            String qualifier = new String(CellUtil.cloneQualifier(cell), StandardCharsets.UTF_8);
                            // 如果列名匹配，则获取其值并拼接
                            if (col.equals(qualifier)) {
                                String res = new String(CellUtil.cloneValue(cell), StandardCharsets.UTF_8);
                                if (data.length() != 0) {
                                    data.append("\\u0001").append(res);
                                }else {
                                    data.append(res);
                                }
                            }
                        }
                    }

                    // 增加输出记录计数
                    hbaseOutputRecords.inc();

                    // 收集查询结果
                    collect(Row.of(propName, key, data.toString()));
                }

            } catch (IOException e) {
                // 日志记录异常信息
                LOG.error(e.getMessage());
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (null != connection) {
            connection.close();
        }
    }

    /**
     * init hbase conf
     * @return
     */
    private static Configuration getHBaseConf() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "linux121:2181");
        return conf;
    }
}
