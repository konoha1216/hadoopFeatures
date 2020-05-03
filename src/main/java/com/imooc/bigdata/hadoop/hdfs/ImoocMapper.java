package com.imooc.bigdata.hadoop.hdfs;

/*
    self-designed Mapper
 */
public interface ImoocMapper {
    /**
     *
     * @param line  every line of the data
     * @param context   cache
     */
    public void map(String line, ImoocContext context);
}
