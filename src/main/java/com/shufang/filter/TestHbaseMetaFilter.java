package com.shufang.filter;


import com.shufang.charactor01.TestHbaseConnection;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Hbase的过滤器除了列值过滤器之外还有MetaData相关的过滤器
 *
 * @FamilyFilter：列族过滤器，但是官方建议还是使用scan.addFamily()的方式比较好；
 * @QualifierFilter：列过滤器，但是官方还是建议使用scan.addColumn()的方式会比较好；
 * @ColumnPrefixFilter：列前缀过滤器，将指定前缀的columnQualifier的所有列进行选定匹配；
 * @MutipleColumnPrefixFilter：可以同时匹配多个前缀对应的所有Column的数据，是对上者的一种补充； 具体可以参考以下实例代码
 * @ColumnRangeFilter：用来从大量列中筛选出所需的列；
 */
public class TestHbaseMetaFilter {


    public static void main(String[] args) throws IOException {

        //使用工具创建一个Connection对象
        Connection connection = TestHbaseConnection.getConnection(HBaseConfiguration.create());
        //获取Table的实例
        Table table = connection.getTable(TableName.valueOf("test1"));

        byte[] family = Bytes.toBytes("cf");
        byte[] qualifier = Bytes.toBytes("cq");


        //创建scan，配置family和column
        Scan scan = new Scan();
        //scan.addColumn(family, qualifier);
        scan.setFilter(new FamilyFilter(CompareOperator.EQUAL,new BinaryComparator(family))); //设置列族过滤器
        scan.setFilter(new QualifierFilter(CompareOperator.EQUAL,new BinaryComparator(qualifier))); //设置列过滤器


        /**
         * @ColumnPrefixFilter
         */
        ColumnPrefixFilter abc = new ColumnPrefixFilter(Bytes.toBytes("abc"));
        //scan.setFilter(abc);


        /**
         * @MultipleColumnPrefixFilter
         */
        byte[][] prefixs = {Bytes.toBytes("abc"),Bytes.toBytes("xyz")};
        MultipleColumnPrefixFilter multipleColumnPrefixFilter = new MultipleColumnPrefixFilter(prefixs);
        scan.setFilter(multipleColumnPrefixFilter);

        /**
         * @ColumnRangeFilter：假如Table有100W个列，我只需要[minColumn,macCloumn)区间的列的数据；
         */
        ColumnRangeFilter columnRangeFilter = new ColumnRangeFilter(Bytes.toBytes("minColumn"),
                true, Bytes.toBytes("maxColumn"), false);
        scan.setFilter(columnRangeFilter);


        /**
         * 然后开始扫描结果，并处理获取到的结果
         */
        ResultScanner rs = table.getScanner(scan);
        for (Result r = rs.next(); r != null; r = rs.next()) {
            for (Cell cell : r.rawCells()) {
                //每个Cell就代表Column与Row的交接处的一个单元，一个Cell可能有多个Version的value，默认返回最大时间戳的
                byte[] value = CellUtil.cloneValue(cell);
                //现在就可以处理获取到的结果value了
            }
        }

        rs.close(); //释放内存资源

    }


}
