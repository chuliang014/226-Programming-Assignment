package edu.ucr.cs.cs226.czhan187;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * Hello world!
 *
 */
public class HDFSUpload
{

    static Configuration conf = new Configuration();

//    copy file between hdfs and local
    public static long copyFile(String local, String hdfs, int sign) throws Exception {

        conf.set("fs.defaultFS", "hdfs://localhost:8020");

        FileSystem localFS = FileSystem.get(conf);
        FileSystem hdfsFS = FileSystem.get(conf);

        Path localPath = new Path(local);
        Path hdfsPath = new Path(hdfs);

        FSDataOutputStream output;
        FSDataInputStream input;


        if(sign == 0){//local -> hdfs
            if(hdfsFS.exists(hdfsPath)){
                throw new Exception("Can't upload to HDFS because file already exists");
            }
            if(!localFS.exists(localPath)){
                throw new Exception("Can't upload to HDFS because file can't be found in local system");
            }
            input = localFS.open(localPath);
            output = hdfsFS.create(hdfsPath);
            System.out.println("Copying the file from local to HDFS");
        }else if(sign == 1){ // hdfs -> local
            if(!hdfsFS.exists(hdfsPath)){
                throw new Exception("Can't upload to local because file can't be found in HDFS");
            }
            input = hdfsFS.open(hdfsPath);
            output = localFS.create(new Path(local + "_temp"));
            System.out.println("Copying the file from HDFS to local");
        }else if(sign == 2){ // local -> local
            if(!localFS.exists(localPath)){
                throw new Exception("Can't upload to local because file can't be found in local system");
            }
            input = localFS.open(localPath);
            output = localFS.create(new Path(local + "_temp"));
            System.out.println("Copying the file from local to local");
        }else{
            throw new Exception("The sign value is invalid");
        }

        long startTime = System.currentTimeMillis();
        IOUtils.copyBytes(input, output,4096,true);
        long endTime = System.currentTimeMillis();

        if(sign == 1 || sign ==2){
            localFS.delete(new Path(local + "_temp"));
        }
        return endTime - startTime;
    }

    public static void main( String[] args )
    {
        String localPath = "/Users/vincent/Learning-Data/cs226/226-Programming-Assignment/HDFSUpload/AREAWATER.csv";
        String hdfsPath = "hdfs://localhost:8020/AREAWATER.csv";
        try {
            long localToHDFS = HDFSUpload.copyFile(localPath, hdfsPath, 0);
            long hdfsToLocal = HDFSUpload.copyFile(localPath, hdfsPath, 1);
            long localToLocal = HDFSUpload.copyFile(localPath, hdfsPath, 2);

            System.out.println("Copying File from Local to HDFS needs: " + localToHDFS +"ms");
            System.out.println("Copying File from HDFS to Local needs: " + hdfsToLocal +"ms");
            System.out.println("Copying File from Local to Local needs: " + localToLocal +"ms");


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
