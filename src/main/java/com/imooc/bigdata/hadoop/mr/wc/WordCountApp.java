package com.imooc.bigdata.hadoop.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * Driver：配置Mapper，Reducer的相关属性
 * 提交到本地运行：开发过程中
 */
public class WordCountApp {
    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "hadoop");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://10.2.64.160:8020");


        // 创建一个Job
        Job job = Job.getInstance(configuration);

        // 设置Job对应的参数
        job.setJarByClass(WordCountApp.class);

        // 设置自定义mapper和reducer的类名
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置自定义mapper和reducer的key，value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 如何输出目录已经存在，需要先删除
        FileSystem fs = FileSystem.get(new URI("hdfs://10.2.64.160:8020"), configuration, "hadoop");
        Path outputPath = new Path("/wordcount/output");
        if(fs.exists(outputPath)){
            fs.delete(outputPath);
        }

        // 设置作业输入和输出的路径
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        FileOutputFormat.setOutputPath(job,outputPath);

        // 提交Job
        boolean result = job.waitForCompletion(true);

        System.exit(result? 0 : -1);

    }
}
