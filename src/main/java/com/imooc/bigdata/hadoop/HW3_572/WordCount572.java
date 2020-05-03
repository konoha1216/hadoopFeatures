package com.imooc.bigdata.hadoop.HW3_572;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class WordCount572 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private Text wordText = new Text();
        private final static Text document = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().toLowerCase().split("\\t",2);
            String documentName = line[0];
            document.set(documentName);
            String textStr = line[1];
            String content = textStr.replaceAll("[^a-z]+", " ");
            StringTokenizer itr = new StringTokenizer(content);

            while(itr.hasMoreTokens()){
                wordText.set(itr.nextToken());
                context.write(wordText, document);
            }

        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String result = "";
            HashMap<String, Integer> m = new HashMap<>();

            for (Text t : values) {
                String str = t.toString();
                m.put(str, m.getOrDefault(str, 0) + 1);
            }

            for (Map.Entry<String, Integer> entry : m.entrySet()) {
                result += entry.getKey() + ":" + entry.getValue() + " ";
            }

            context.write(key, new Text(result));
        }

    }


    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "hadoop");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://10.2.64.160:8020");

        Job job = Job.getInstance(configuration, "word count");
        job.setJarByClass(WordCount572.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem fs = FileSystem.get(new URI("hdfs://10.2.64.160:8020"), configuration, "hadoop");
        Path outputPath = new Path("/572HW3/output/");
        if (fs.exists(outputPath)) {
            fs.delete(outputPath);
        }

        FileInputFormat.addInputPath(job, new Path("/572HW3/input/"));
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
