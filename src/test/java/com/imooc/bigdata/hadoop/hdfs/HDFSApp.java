package com.imooc.bigdata.hadoop.hdfs;

import com.amazonaws.services.elasticmapreduce.model.Ec2InstanceAttributes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sun.nio.ch.IOUtil;

import javax.security.auth.login.AppConfigurationEntry;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

public class HDFSApp {

    public static final String HDFS_PATH="hdfs://hadoop000:8020";
    FileSystem fileSystem=null;
    Configuration configuration=null;

    @Before
    public void setUp() throws Exception{
        System.out.println("---setUp---");
        configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "hadoop");
    }

    /*check the replication number of a file*/
    @Test
    public void testReplication(){
        System.out.println(configuration.get("dfs.replication"));
    }

    /*create the directory*/
    @Test
    public void mkdir() throws Exception{
        Path path = new Path("/hdfsapi3/test_junit");
        boolean result = fileSystem.mkdirs(path);
        System.out.println(result);
    }

    /*show the file content*/
    @Test
    public void text() throws Exception{
        Path path = new Path("/README.txt");
        FSDataInputStream f = fileSystem.open(path);
        IOUtils.copyBytes(f, System.out, 1024);
    }

    /*create a file*/
    @Test
    public void write() throws Exception{
//        Path path = new Path("/hdfsapi3/test_junit/a.txt");
        Path path = new Path("/hdfsapi3/test_junit/b.txt");
        FSDataOutputStream f = fileSystem.create(path);
        f.writeUTF("hello world! replication=1");
        f.flush();
        f.close();
    }

    /*
    rename a file
     */
    @Test
    public void rename()throws Exception{
        Path oldPath = new Path("/hdfsapi3/test_junit/b.txt");
        Path newPath = new Path("/hdfsapi3/test_junit/c.txt");
        boolean result = fileSystem.rename(oldPath, newPath);
        System.out.println(result);
    }

    /*
    copy a file from the local env
     */
    @Test
    public void copyFromLocalFile()throws Exception{
        Path src = new Path("/Users/konoha/Desktop/lc.txt");
        Path dst = new Path("/hdfsapi3/test_junit/hadoop_lc.txt");
        fileSystem.copyFromLocalFile(src, dst);
    }

    /*
    copy a file to the local env
     */
    @Test
    public void copyToLocalFile()throws Exception{
        Path src = new Path("/hdfsapi3/test_junit/c.txt");
        Path dst = new Path("/Users/konoha/Desktop/c.txt");
        fileSystem.copyToLocalFile(src, dst);
    }

    /*
    copy a local big file to the server
     */
    @Test
    public void copyFromLocalBigFile()throws Exception{
        InputStream in = new BufferedInputStream(new FileInputStream(new File("/Users/konoha/Desktop/train.txt")));
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi3/test_junit/bio_train.txt"),
                new Progressable() {
                    @Override
                    public void progress() {
                        System.out.print("-");
                    }
                });

        IOUtils.copyBytes(in, out, 1024);

    }

    /*
    list all file and their status
     */
    @Test
    public void listFiles()throws Exception{
        FileStatus[] files = fileSystem.listStatus(new Path("/hdfsapi3/test_junit/"));
        for(FileStatus file:files){
            String isDir = file.isDirectory() ? "directory":"file";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long length = file.getLen();
            String path = file.getPath().toString();
            System.out.println(isDir + '\t' + permission + '\t' +replication + '\t' + length + '\t' + path);
        }
    }

    /*
    list all file and their status under one directory recursively
     */

    @Test
    public void listFileRecursive()throws Exception{
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/hdfsapi3/test_junit/"), true);
        while(files.hasNext()){
            LocatedFileStatus file = files.next();
            String isDir = file.isDirectory() ? "directory":"file";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long length = file.getLen();
            String path = file.getPath().toString();
            System.out.println(isDir + '\t' + permission + '\t' +replication + '\t' + length + '\t' + path);
        }
    }

    /*
    get the file's blocks info
     */
    @Test
    public void getFileBlockLocations()throws Exception{
        FileStatus file = fileSystem.getFileStatus(new Path("/hdfsapi3/test_junit/a.txt"));
        BlockLocation[] locations = fileSystem.getFileBlockLocations(file,0, file.getLen());
        for (BlockLocation block:locations){
            for (String name: block.getNames()){
                System.out.println(name + '\t' + block.getOffset() + '\t' + block.getLength() + '\t' + block.getHosts().toString());
            }
        }
    }

    /*
    delete a file
     */
    @Test
    public void deleteFile()throws Exception{
        boolean result = fileSystem.delete(new Path("/hdfsapi3/test_junit/bio_train.txt"), true);
        System.out.println(result);
    }



    @After
    public void tearDown(){
        System.out.println("---tearDown---");
        fileSystem=null;
        configuration=null;
    }

    public static void main(String[] args) throws Exception{

//        Configuration configuration = new Configuration();
//        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop000:8020"), configuration, "hadoop");
//        Path path = new Path("/hdfsapi2/test2");
//        boolean result = fileSystem.mkdirs(path);
//        System.out.println(result);
    }
}
