package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.fs.Path;

public class PathUtil {
  public static String suffix=".hashtable";
  public static String generatePath(String baseURI,Byte tag,String bigBucketFileName){
    String path = new String(baseURI+Path.SEPARATOR+"-"+tag+"-"+bigBucketFileName+suffix);
    return path;
  }
  public static String generateFileName(Byte tag,String bigBucketFileName){
    String fileName = new String("-"+tag+"-"+bigBucketFileName+suffix);
    return fileName;
  }

  public static String generateTmpURI(String baseURI,String id){
    String tmpFileURI = new String(baseURI+Path.SEPARATOR+"HashTable-"+id);
    return tmpFileURI;
  }
}
