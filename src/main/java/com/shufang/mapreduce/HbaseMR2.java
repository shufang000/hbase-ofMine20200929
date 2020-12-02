package com.shufang.mapreduce;


import com.google.inject.internal.cglib.proxy.$Callback;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 该程序主要是从HDFS的文件读取数据插入到Hbase的表中
 */
public class HbaseMR2 {

    public static void main(String[] args) throws Exception {

        //建立Hbase的配置对象
        Configuration conf = HBaseConfiguration.create();

        //提交job并运行
        int status = ToolRunner.run(conf, new MyHDFSRunner(), args);

        System.exit(status);

    }


    //由于需要从HDFS的文件读取数据，所以不能用TableMapper，因为它是专门针对Hbase数据源的Mapper
    class MyHDFSFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //从HDFS读取的一行数据，默认按照TextInputFormat读取
            String line = value.toString();
            String[] words = line.split("\t");

            //获取读取的内容
            String rowkey = words[0];
            String name = words[1];
            String color = words[2];

            ImmutableBytesWritable rowkeyWritable = new ImmutableBytesWritable(Bytes.toBytes(rowkey));
            Put put = new Put(Bytes.toBytes(rowkey));

            //在Put对象中添加内容
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(color));

            //将对象封装好从map端通过context对象进行输出
            context.write(rowkeyWritable, put);

            super.map(key, value, context);
        }
    }

    class MyHDFSFileReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {

            for (Put value : values) {
                context.write(NullWritable.get(), value);
            }


            super.reduce(key, values, context);
        }
    }

    static class MyHDFSRunner extends Configured implements Tool {
        public int run(String[] strings) throws Exception {
            Configuration conf = this.getConf();

            Job job = Job.getInstance(conf, this.getClass().getSimpleName());

            //设置Mapper
            job.setMapperClass(MyHDFSFileMapper.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(Put.class);


            //设置Reducer

            TableMapReduceUtil.initTableReducerJob("table_name_mr", MyHDFSFileReducer.class, job);


            job.setNumReduceTasks(1);

            boolean isSuccess = job.waitForCompletion(true);

            if (!isSuccess) {
                throw new Exception("run error when execute the job");
            }

            //返回三目运算结果
            return isSuccess ? 1 : 0;
        }
    }
}
