package com.shufang.filter;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


/**
 * 常用的Column-Value相关的过滤器有：
 * @FilterList： 可以用来装在多个过滤器，同时可选择对所有的Filter的配置MUST_PASS_ONE 与 MUST_PASS_ALL之间的过滤规则；
 * @ColumnValueFilter： 返回匹配的Cell，是Hbase在2.0.0的时候对之前版本SingleColumnValueFilter的补充；
 * @SingleColumnValueFilter： 返回匹配到的所有value的所在row的所有数据内容；
 * @ValueFilter： 这个需要在scan的时候预先.addFamily().addColumn(),一般用于简单的查询类似于where$name=name；
 *
 * 一般的列值过滤器都是配合不同的列值比较器进行使用，常用的列值比较器如下。
 * @RegexStringComparator： 正则字符串比较器
 * @SubStringComparator： 子字符串比较器
 * @BinaryPrefixComparator： 二进制前缀比较器
 * @BinaryComparator： 二进制比较器
 * @BinaryComponentComparator： 二进制组建比较器，用特定位置的特定值与Cell进行比较，可以对比ascii和binary数据
 *
 */
public class TestColumnValueFilter {


    public static void main(String[] args) throws IOException {


        Connection connection = ConnectionFactory.createConnection();

        //创建过滤器列表的实例，可以装在多个实例
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);


        //预先准备需要扫描的cf和column-qualifier
        byte[] cf = Bytes.toBytes("");
        byte[] cq = Bytes.toBytes("");
        byte[] value1 = Bytes.toBytes("SuperMan");
        byte[] value2 = Bytes.toBytes("BiteMan");


        Filter filter1 = TestColumnValueFilter.getSingleColumnValueFilter(cf, cq, value1);
        Filter filter2 = TestColumnValueFilter.getSingleColumnValueFilter(cf, cq, value1);

        //添加过滤器
        filterList.addFilter(filter1);
        filterList.addFilter(filter2);

        //获取table的实例
        Table table = connection.getTable(TableName.valueOf(""));

        Scan scan = new Scan();
        scan.setFilter(filterList);
        scan.setBatch(10000);
        ResultScanner scanner = table.getScanner(scan);

        //然后对scan的扫描结果进行操作


        /**
         * 对于简单的过滤条件，如：$value = cf:col:value,这种,官方强烈推荐使用ValueFilter,
         * 而不是
         * @ColumnValueFilter and
         * @SingleColumnValueFilter
         * 实例代码如下
         */
        scan.addColumn(cf,cq); //限定扫描额ColumnFamily和Column，避免扫描无关的列
        scan.setFilter(new ValueFilter(CompareOperator.EQUAL,new BinaryComparator(value1)));



        /**
         * 除了对特定value值的过滤支持，ColumnValueFilter还支持另外一种构造参数的过滤方式。
         * @ColumnValueFilter(
         *                  final byte[] family,
         *                  final byte[] qualifier,
         *                  final CompareOperator op,
         *                  final ByteArrayComparable comparator
         *                  TODO 这个参数是一种value的拓展比较方式，可以使用正则、字符串截取进行匹配
         *                  )
         * @regexStringComparator  正则匹配字符串
         * @substringComparator
         */

        RegexStringComparator regexStringComparator = new RegexStringComparator("my."); //所有以my开头的row
        new SingleColumnValueFilter(cf,cq,CompareOperator.EQUAL,regexStringComparator);

        SubstringComparator substringComparator = new SubstringComparator("y val"); //所有包含y val的row，my value会匹配出来
        new SingleColumnValueFilter(cf,cq,CompareOperator.EQUAL,substringComparator);


    }

    /**
     * @SingleColumnValueFilter ：对特定value值的所有row进行等值，不等值，或者范围range的筛选
     * CompareOperator.EQUAL
     * CompareOperator.NOT_EQUAL
     * CompareOperator.GREATER
     * @param cf
     * @param cq
     * @param value
     * @return SingleColumnValueFilter
     */
    public static  Filter getSingleColumnValueFilter(byte[] cf,byte[] cq,byte[] value){
        return new SingleColumnValueFilter(cf,cq,CompareOperator.EQUAL,value);
    }


    /**
     * @ColumnValueFilter :在Hbase2.0.0之后，引入了ColumnValueFilter对SingleColumnValueFilter作补充，
     * 仅获取匹配到的value所在的Column的Cell，SingleColumnValueFilter会获取到匹配到的value的所在ROW的所
     * 有数据，包括其它列
     * TODO ColumnValueFilter的构造器参数与SingleColumnValueFilter是保持一致的
     * @param cf
     * @param cq
     * @param value
     * @return ColumnValueFilter
     */
    public static  Filter getColumnValueFilter(byte[] cf,byte[] cq,byte[] value){
        return new ColumnValueFilter(cf,cq,CompareOperator.EQUAL,value);

    }








}
