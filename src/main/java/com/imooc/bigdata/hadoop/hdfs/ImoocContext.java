package com.imooc.bigdata.hadoop.hdfs;

import java.util.HashMap;
import java.util.Map;

/*
    cache
 */
public class ImoocContext {

    private Map<Object, Object> cacheMap = new HashMap<Object, Object>();

    public Map<Object, Object> getCacheMap(){
        return cacheMap;
    }

    /*
    write the data into the cache
     */
    public void write(Object key, Object value){
        cacheMap.put(key, value);
    }

    /*
    read the cache
     */
    public Object get(Object key){
        return cacheMap.get(key);
    }
}
