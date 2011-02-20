/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.contrib.serde2.s3;

/**
 * S3LogStruct.
 *
 */
public class S3LogStruct {

  public String bucketowner;
  public String bucketname;
  public String rdatetime;
  // public Long rdatetimeepoch; // The format Hive understands by default,
  // should we convert?
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
  // public String rid; // Specific Zemanta use
}
