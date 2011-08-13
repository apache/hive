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

import java.io.BufferedOutputStream;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;

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
  // Size of string buffer in bytes
  private static final int STRING_BUFFER_SIZE = 16 * 1024;
  // The size to flush the string buffer at
  private static final int STRING_BUFFER_FLUSH_SIZE = 14 * 1024;

  // Size of stdout buffer in bytes
  private static final int STDOUT_BUFFER_SIZE = 128 * 1024;
  // In verbose mode, print an update per RECORD_PRINT_INTERVAL records
  private static final int RECORD_PRINT_INTERVAL = (1024*1024);

  public RCFileCat() {
    super();
    decoder = Charset.forName("UTF-8").newDecoder().
      onMalformedInput(CodingErrorAction.REPLACE).
      onUnmappableCharacter(CodingErrorAction.REPLACE);
  }

  private static CharsetDecoder decoder;

  Configuration conf = null;

  private static String TAB ="\t";
  private static String NEWLINE ="\r\n";

  @Override
  public int run(String[] args) throws Exception {
    long start = 0l;
    long length = -1l;
    int recordCount = 0;
    long startT = System.currentTimeMillis();
    boolean verbose = false;

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
      } else if (arg.equals("--verbose")) {
        verbose = true;
      } else if (fileName == null){
        fileName = new Path(arg);
      } else {
        printUsage(null);
      }
    }

    setupBufferedOutput();
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
    StringBuilder buf = new StringBuilder(STRING_BUFFER_SIZE); // extra capacity in case we overrun, to avoid resizing
    while (recordReader.next(key, value)) {
      printRecord(value, buf);
      recordCount++;
      if (verbose && (recordCount % RECORD_PRINT_INTERVAL) == 0) {
        long now = System.currentTimeMillis();
        System.err.println("Read " + recordCount/1024 + "k records");
        System.err.println("Read " + ((recordReader.getPos() / (1024L*1024L)))
                                                                      + "MB");
        System.err.printf("Input scan rate %.2f MB/s\n",
                  (recordReader.getPos() * 1.0 / (now - startT)) / 1024.0);
      }
      if (buf.length() > STRING_BUFFER_FLUSH_SIZE) {
        System.out.print(buf.toString());
        buf.setLength(0);
      }
    }
    // print out last part of buffer
    System.out.print(buf.toString());
    System.out.flush();
    return 0;
  }


  /**
   * Print record to string builder
   * @param value
   * @param buf
   * @throws IOException
   */
  private void printRecord(BytesRefArrayWritable value, StringBuilder buf)
      throws IOException {
    int n = value.size();
    if (n > 0) {
      BytesRefWritable v = value.unCheckedGet(0);
      ByteBuffer bb = ByteBuffer.wrap(v.getData(), v.getStart(), v.getLength());
      buf.append(decoder.decode(bb));
      for (int i = 1; i < n; i++) {
        // do not put the TAB for the last column
        buf.append(RCFileCat.TAB);

        v = value.unCheckedGet(i);
        bb = ByteBuffer.wrap(v.getData(), v.getStart(), v.getLength());
        buf.append(decoder.decode(bb));
      }
      buf.append(RCFileCat.NEWLINE);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  private static String Usage = "RCFileCat [--start=start_offet] [--length=len] [--verbose] fileName";

  public static void main(String[] args) {
    try {

      Configuration conf = new Configuration();
      RCFileCat instance = new RCFileCat();
      instance.setConf(conf);

      ToolRunner.run(instance, args);
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("\n\n\n");
      printUsage(e.getMessage());
    }
  }

  private static void setupBufferedOutput() {
    FileOutputStream fdout =
        new FileOutputStream(FileDescriptor.out);
    BufferedOutputStream bos =
        new BufferedOutputStream(fdout, STDOUT_BUFFER_SIZE);
    PrintStream ps =
        new PrintStream(bos, false);
    System.setOut(ps);
  }
  private static void printUsage(String errorMsg) {
    System.err.println(Usage);
    if(errorMsg != null) {
      System.err.println(errorMsg);
    }
    System.exit(1);
  }

}
