package com.shufang.filter;


import com.shufang.charactor01.TestHbaseConnection;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

import java.io.IOException;

/**
 * Hbase除了列值、row、元数据过滤器之外，还有工具型的过滤器
 *
 * @FirstKeyOnlyFilter：只会返回每行的第一个KV，这里的KV代表一个Cell，在hbase需要进行rowcount的时候很实用
 */
public class TestUtilityFilter {
    public static void main(String[] args) throws IOException {

        Connection conn = TestHbaseConnection.getConnection(HBaseConfiguration.create());


        Table table = conn.getTable(TableName.valueOf(""));
        ResultScanner rs = null;
        try {

            Scan scan = new Scan();
            scan.setFilter(new FirstKeyOnlyFilter());
            rs = table.getScanner(scan);

            //handle the rs with rs.next()
            Long sum = 0l;

            for (Result r = rs.next(); r != null ; r= rs.next()) {
                sum+=1;

                Cell[] cells = r.rawCells();
                for (Cell cell : cells) {
                    //处理cell中的数据
                    byte[] value = CellUtil.cloneValue(cell);
                    cell.getTimestamp(); //查看Cell对应的版本时间戳。
                    //.......
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            rs.close();
            table.close();
        }


    }
}
