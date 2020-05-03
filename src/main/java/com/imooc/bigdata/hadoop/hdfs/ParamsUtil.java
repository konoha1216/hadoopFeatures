package com.imooc.bigdata.hadoop.hdfs;

import java.io.IOException;
import java.util.Properties;

public class ParamsUtil {

    private static Properties properties = new Properties();
    static {
        try {
            properties.load(ParamsUtil.class.getClassLoader().getResourceAsStream("wc.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Properties getProperties() throws Exception{
        return properties;
    }

//    public static void main(String[] args) throws Exception{
//        System.out.println(getProperties().getProperty("INPUT_PATH"));
//    }

}
