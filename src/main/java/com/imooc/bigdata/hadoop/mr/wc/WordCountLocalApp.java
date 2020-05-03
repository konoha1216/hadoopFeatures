package com.imooc.bigdata.hadoop.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Driver：配置Mapper，Reducer的相关属性
 * 本地文件->输出到本地
 */
public class WordCountLocalApp {
    public static void main(String[] args) throws Exception {

//        System.setProperty("HADOOP_USER_NAME", "hadoop");

        Configuration configuration = new Configuration();
//        configuration.set("fs.defaultFS", "hdfs://10.2.64.160:8020");


        // 创建一个Job
        Job job = Job.getInstance(configuration);

        // 设置Job对应的参数
        job.setJarByClass(WordCountLocalApp.class);

        // 设置自定义mapper和reducer的类名
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置combiner
        job.setCombinerClass(WordCountReducer.class);

        // 设置自定义mapper和reducer的key，value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置作业输入和输出的路径
        FileInputFormat.setInputPaths(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        //提交Job
        boolean result = job.waitForCompletion(true);

        System.exit(result? 0 : -1);

    }
}
