package com.imooc.bigdata.hadoop.hdfs;

/**
 * self-designed implementation
 */
public class CaseLowerWordCountMapper implements ImoocMapper{

    @Override
    public void map(String line, ImoocContext context) {
        String[] words = line.replaceAll("[\\pP\\p{Punct}]","").toLowerCase().split(" ");
        for(String word:words){
            Object value = context.get(word);
            if(value==null){
                context.write(word, 1);
            }else{
                int v = Integer.parseInt(value.toString());
                context.write(word, v+1);
            }
        }
    }
}
