package com.shufang.charactor01;

import com.sun.org.apache.xpath.internal.operations.String;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

/**
 * 在Hbase的JavaAPI中，
 * admin是用来进行DDL操作的
 * table是用来进行DML操作的，主要是CRUD
 */
public class TestHbaseConnection {
    //用来装连接对象的容器
    private static ThreadLocal<Connection> connectionThreadLocal = new ThreadLocal<Connection>();
    //private static ThreadLocal<Admin> adminThreadLocal = new ThreadLocal<Admin>();

    /**
     * 这是获取一个重量级Hbase连接对象的方法
     * @param conf
     * @return
     * @throws IOException
     */
    public static Connection getConnection(Configuration conf) throws IOException {
        //创建一个共享连接Connection,因为该连接对象属于重量级的对象，记得不用的时候请关闭
        Connection connection = connectionThreadLocal.get();

        if (null == connection) {
            connection = ConnectionFactory.createConnection(conf);
            connectionThreadLocal.set(connection);
        }

        return connection;
    }

    public static void main(String[] args) throws IOException {

        //创建对应的HbaseConfiguration实例
        Configuration conf = HBaseConfiguration.create();
        //conf.set("","");

        //创建一个共享连接Connection,因为该连接对象属于重量级的对象，记得不用的时候请关闭
        Connection connection = TestHbaseConnection.getConnection(conf);

        // Table属于轻量级实例，可以使用同一个连接对数据库中的多个Table进行操作
        Table table = connection.getTable(TableName.valueOf("test"));


        //创建Table的实例之后，将交互的代码包在try-catch-finally中
        try {
            //在这里就可以很开心的使用Table实例进行数据库的操作了，单线程操作Table，线程安全

            byte[] rk = Bytes.toBytes("RowKey001");
            byte[] cf = Bytes.toBytes("cf1");
            byte[] column = Bytes.toBytes("name");
            byte[] value = Bytes.toBytes("SuperMan");


            //往数据库中添加一个值
            Put put = new Put(rk);
            put = put.addColumn(cf, column, value);
            table.put(put);
            //该异常可以自定义
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            table.close();
            connectionThreadLocal.remove(); //从ThreadLocal中关闭
            connection.close();

        }


    }
}
