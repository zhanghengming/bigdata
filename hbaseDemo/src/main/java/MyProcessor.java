import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class MyProcessor extends BaseRegionObserver {
    @Override
    //删之前的操作
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {

        // 获取table对象
        HTableInterface friends = e.getEnvironment().getTable(TableName.valueOf("friends"));

        // table 对象.put 传rowkey 得到监听到的删除对象的key
        byte[] uid1 = delete.getRow();

        // 创建查询对象get
        Get get = new Get(uid1);
        // 指定列族和列信息
        get.addColumn(Bytes.toBytes("friends_info"), Bytes.toBytes("uid"));

        //执行查询
        Result result = friends.get(get);
        // 获取该行的所有cell对象
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            byte[] uid = CellUtil.cloneValue(cell);
            String key = Bytes.toString(CellUtil.cloneRow(cell)) ;
            // 删除指定rowkey
            Delete delete1 = new Delete(uid);
            // 执行删除数据
            friends.delete(delete1);
            friends.close();
        }
        System.out.println("删除成功");


    }
}
