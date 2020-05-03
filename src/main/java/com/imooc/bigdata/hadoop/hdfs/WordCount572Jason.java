package com.imooc.bigdata.hadoop.hdfs;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount572Jason {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{

        private Text word = new Text();
        private String docId = "";
        private boolean flag = true;

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();
            String []parts = line.split("\\t",2);
            docId = parts[0];
            String docContent = parts[1].replaceAll("[^a-z]+", " ");
            StringTokenizer itr = new StringTokenizer(docContent);

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, new Text(docId));
            }
        }
    }

    public static class InvertReducer
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            HashMap<String,Integer> result = new HashMap<>();
            for (Text val : values) {
                String string = val.toString();
                result.put(string, result.getOrDefault(string, 0) + 1);
            }
            StringBuilder sb=new StringBuilder("");
            Iterator mapIter = result.entrySet().iterator();
            while(mapIter.hasNext()){
                sb.append(" ");
                Map.Entry mapElement = (Map.Entry)mapIter.next();
                sb.append(mapElement.getKey()+":"+mapElement.getValue());
            }
            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "hadoop");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://10.2.64.160:8020");

        Job job = Job.getInstance(configuration, "word count");

        job.setJarByClass(WordCount572Jason.class);
        job.setMapperClass(TokenizerMapper.class);
        // job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(InvertReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem fs = FileSystem.get(new URI("hdfs://10.2.64.160:8020"), configuration, "hadoop");
        Path outputPath = new Path("/572HW3/output2/");
        if (fs.exists(outputPath)) {
            fs.delete(outputPath);
        }

        FileInputFormat.addInputPath(job, new Path("/572HW3/input/"));
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
