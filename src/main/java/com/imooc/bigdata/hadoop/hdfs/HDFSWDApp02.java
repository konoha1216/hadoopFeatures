package com.imooc.bigdata.hadoop.hdfs;

/*
    HDFS API to do the wordcount job

    do the wc job on a file in HDFS, then output the result to HDFS

    1) read in the file in the HDFS
    2) wordcount: every line with the separator
    3) save the result
    4) output the result to the HDFS


    plugin method
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class HDFSWDApp02 {
    public static void main(String[] args)throws Exception {
//        1)

        Properties properties = ParamsUtil.getProperties();

        Path input = new Path(properties.getProperty(Constants.INPUT_PATH));

        FileSystem fs = FileSystem.get(new URI(properties.getProperty(Constants.HDFS_URI)), new Configuration(), "hadoop");

        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(input,false);

        Class<?> clazz = Class.forName(properties.getProperty(Constants.MAPPER_CALSS));
        ImoocMapper mapper = (ImoocMapper)clazz.newInstance();
//        ImoocMapper mapper = new WordCountMapper();
        ImoocContext context = new ImoocContext();

        while(iterator.hasNext()){
            LocatedFileStatus file = iterator.next();
            FSDataInputStream in =fs.open(file.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            String line = "";
            while((line=reader.readLine()) != null){
//                2)
                mapper.map(line, context);
            }

            reader.close();
            in.close();
        }


//        3) Map
        Map<Object, Object> contextMap = context.getCacheMap();

//        4)
        Path output = new Path(properties.getProperty(Constants.OUTPUT_PATH));
        FSDataOutputStream out = fs.create(new Path(output, new Path(properties.getProperty(Constants.OUTPUT_FILE))));

        Set<Map.Entry<Object, Object>> entries = contextMap.entrySet();
        for(Map.Entry<Object, Object> entry: entries){
            out.write((entry.getKey().toString() + "\t" + entry.getValue() + "\n").getBytes());
        }

        out.close();
        fs.close();

        System.out.println("run success");
    }
}
