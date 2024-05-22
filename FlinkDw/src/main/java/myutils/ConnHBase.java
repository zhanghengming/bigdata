package myutils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class ConnHBase {

    public ConnHBase() {}

    public Connection connToHbase() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","linux121,linux122,linux123");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,30000);
        conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,30000);
        try {
           return ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
