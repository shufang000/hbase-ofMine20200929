package com.shufang.filter;

import com.shufang.charactor01.TestHbaseConnection;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 通常Hbase对行的过滤，可以通过startRow与stopRow进行过滤（但是在Hbase2.X后续版本中标示为过时）,
 * 但是通过RowFilter也是可以进行过滤操作的！
 *
 * @RowFilter
 */
public class TestRowFilter {
    public static void main(String[] args) throws IOException {

        Connection conn = TestHbaseConnection.getConnection(HBaseConfiguration.create());
        Table table = conn.getTable(TableName.valueOf("test"));


        /**
         * 1、常规的通过scan的原生方法进行过滤
         */
        Scan scan = new Scan();
        //scan.setStartRow()
        //scan.setStopRow()

        /**
         * 2、通过RowFilter进行过滤
         * @RowFilter可以配合@BinaryComponentComparator等比较器进行配合使用，还能扩展过滤功能
         */
        scan.setRowPrefixFilter(Bytes.toBytes("prefix1"));//按照Row的前缀进行过滤匹配
        scan.setFilter(new RowFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("momoji"))));

        ResultScanner scanner = table.getScanner(scan);
        for (Result r = scanner.next(); r != null ; r= scanner.next()) {

            Cell[] cells = r.rawCells();

            for (Cell cell : cells) {
                //处理cell中的数据
                byte[] value = CellUtil.cloneValue(cell);
                cell.getTimestamp(); //查看Cell对应的版本时间戳。
                //.......
            }
        }

        scanner.close();//释放资源
    }
}
