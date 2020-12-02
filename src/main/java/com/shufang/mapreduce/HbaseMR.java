package com.shufang.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class HbaseMR {
    public static void main(String[] args) throws Exception {

        //该类可以将数据写入到Hbase的Table中
        //TableOutputFormat

        //HFileOutputFormat2.configureIncrementalLoad();

        MyRunner runner = new MyRunner();
        Configuration conf = HBaseConfiguration.create();
        //执行程序
        ToolRunner.run(conf,runner,args);

    }


    /**
     * 创建一个Mapper类
     */
    class MyMapper extends TableMapper<ImmutableBytesWritable, Put> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            //TODO with the params,output the data to reducer or straight to the file
            Put put = new Put(key.get());

            for (Cell cell : value.rawCells()) {
                //判断收集过来的数据是否满足我们的要求
                if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {

                    if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                        //将Cell添加到put
                        put.add(cell);
                    } else if ("color".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                        //将Cell添加到put
                        put.add(cell);
                    }
                }

                //将key、put作为map端的输出结果写入到Reduce端
                context.write(key, put);
            }
        }
    }

    /**
     * 创建一个Reducer类
     */
    class MyReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
            //TODO output the data to the output dir
            for (Put put : values) {
                //将结果输出
                context.write(NullWritable.get(), put);
            }
        }
    }


    //创建一个工具类，组装Mapreduce job
    static class MyRunner extends Configured implements Tool {

        //需要实现这个方法
        public int run(String[] strings) throws Exception {

            //获取配置
            Configuration conf = this.getConf();

            //创建Job
            Job job = Job.getInstance(conf, this.getClass().getSimpleName());

            //配置job
            Scan scan = new Scan();
            scan.setCacheBlocks(true);
            scan.setBatch(3000);

            //通过工具类配置Mapper，是mapreduce新包下面的而不是mapred
            TableMapReduceUtil.initTableMapperJob(
                    "table_name",
                    scan,
                    MyMapper.class,
                    NullWritable.class,
                    Put.class,
                    job,
                    false
            );

            //通过工具类配置Mapper
            TableMapReduceUtil.initTableReducerJob(
                    "table_name_mr",
                    MyReducer.class,
                    job
            );

            //设置reducer task的数量，最少一个
            job.setNumReduceTasks(1);

            boolean isSuccess = job.waitForCompletion(true);
            if (!isSuccess) {
                throw new Exception("job running in error~");
            }

            return isSuccess ? 1 : 0;
        }
    }
}
