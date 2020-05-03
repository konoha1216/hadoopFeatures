package com.imooc.bigdata.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *  KEYIN   MAP任务读数据的key类型，offset，每行数据起始位置的偏移量，Long
 *  VALUEIN MAP任务读数据的value类型，一行行字符串，String
 *
 *  KEYOUT  MAP方法自定义实现输出的key的类型，String
 *  VALUEOUT    MAP方法自定义实现输出的value类型，Integer
 *
 *  词频统计：相同单词的次数  (word, 1)
 *
 *  但是Long, String, Integer是java里的数据类型
 *  Hadoop自定义类型：序列化和反序列化
 *
 *  LongWritable,Text, IntWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\t");

        for (String word: words){
            context.write(new Text(word.toLowerCase()), new IntWritable(1));
        }
    }
}
