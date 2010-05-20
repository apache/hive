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

package org.apache.hadoop.hive.cli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFileRecordReader;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RCFileCat implements Tool{
  
  Configuration conf = null;
  
  private static String TAB ="\t";
  private static String NEWLINE ="\r\n";

  @Override
  public int run(String[] args) throws Exception {
    long start = 0l;
    long length = -1l;
    
    //get options from arguments
    if (args.length < 1 || args.length > 3) {
      printUsage(null);
    }
    Path fileName = null;
    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      if(arg.startsWith("--start=")) {
        start = Long.parseLong(arg.substring("--start=".length()));
      } else if (arg.startsWith("--length=")) {
        length = Long.parseLong(arg.substring("--length=".length()));
      } else if (fileName == null){
        fileName = new Path(arg);
      } else {
        printUsage(null);
      }
    }
 
    FileSystem fs = FileSystem.get(fileName.toUri(), conf);
    long fileLen = fs.getFileStatus(fileName).getLen();
    if (start < 0) {
      start = 0;
    }
    if (start > fileLen) {
      return 0;
    }
    if (length < 0 || (start + length) > fileLen) {
      length = fileLen - start;
    }
    
    //share the code with RecordReader.
    FileSplit split = new FileSplit(fileName,start, length, new JobConf(conf));
    RCFileRecordReader recordReader = new RCFileRecordReader(conf, split);
    LongWritable key = new LongWritable();
    BytesRefArrayWritable value = new BytesRefArrayWritable();
    Text txt = new Text();
    while (recordReader.next(key, value)) {
      txt.clear();
      for (int i = 0; i < value.size(); i++) {
        BytesRefWritable v = value.get(i);
        txt.set(v.getData(), v.getStart(), v.getLength());
        System.out.print(txt.toString());
        if (i < value.size() - 1) {
          // do not put the TAB for the last column
          System.out.print(RCFileCat.TAB);          
        }
      }
      System.out.print(RCFileCat.NEWLINE);
    }
    return 0;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  private static String Usage = "RCFileCat [--start=start_offet] [--length=len] fileName";
  
  public static void main(String[] args) {
    try {
      Configuration conf = new Configuration();
      RCFileCat instance = new RCFileCat();
      instance.setConf(conf);
      ToolRunner.run(instance, args);
    } catch (Exception e) {
      printUsage(e.getMessage());
    }
  }
  
  private static void printUsage(String errorMsg) {
    System.out.println(Usage);
    if(errorMsg != null) {
      System.out.println(errorMsg);      
    }
    System.exit(1);
  }
  
}
