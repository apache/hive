package org.apache.hadoop.hive.contrib.serde2.s3;

public class S3LogStruct {

  public String bucketowner;
  public String bucketname;
  public String rdatetime;
//  public Long rdatetimeepoch;     //  The format Hive understands by default, should we convert?
  public String rip;
  public String requester;
  public String requestid;
  public String operation;
  public String rkey;
  public String requesturi;
  public Integer httpstatus;
  public String errorcode;
  public Integer bytessent;
  public Integer objsize;
  public Integer totaltime;
  public Integer turnaroundtime;
  public String referer;
  public String useragent;
//  public String rid;  // Specific Zemanta use
}
