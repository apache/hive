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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.junit.Test;

/**
 * test RCFileCat
 *
 */
public class TestRCFileCat {

  /**
   * test parse file
   */
  @Test
  public void testRCFileCat() throws Exception {
    File template = File.createTempFile("hive", "tmpTest");
    Configuration configuration = new Configuration();

    byte[][] record_1 = { Bytes.toBytes("123"), Bytes.toBytes("456"),
        Bytes.toBytes("789"), Bytes.toBytes("1000"), Bytes.toBytes("5.3"),
        Bytes.toBytes("hive and hadoop"), new byte[0], Bytes.toBytes("NULL") };
    byte[][] record_2 = { Bytes.toBytes("100"), Bytes.toBytes("200"),
        Bytes.toBytes("123"), Bytes.toBytes("1000"), Bytes.toBytes("5.3"),
        Bytes.toBytes("hive and hadoop"), new byte[0], Bytes.toBytes("NULL") };
    byte[][] record_3 = { Bytes.toBytes("200"), Bytes.toBytes("400"),
        Bytes.toBytes("678"), Bytes.toBytes("1000"), Bytes.toBytes("4.8"),
        Bytes.toBytes("hive and hadoop"), new byte[0], Bytes.toBytes("TEST") };

    RCFileOutputFormat.setColumnNumber(configuration, 8);

    Path file = new Path(template.getAbsolutePath());

    FileSystem fs = FileSystem.getLocal(configuration);
    RCFile.Writer writer = new RCFile.Writer(fs, configuration, file, null,
        RCFile.createMetadata(new Text("apple"), new Text("block"), new Text(
            "cat"), new Text("dog")), new DefaultCodec());
    write(writer, record_1);
    write(writer, record_2);
    write(writer, record_3);
    writer.close();

    RCFileCat fileCat = new RCFileCat();
    RCFileCat.test=true;
    fileCat.setConf(new Configuration());

    // set fake input and output streams
    PrintStream oldOutPrintStream= System.out;
    PrintStream oldErrPrintStream= System.err;
    ByteArrayOutputStream dataOut= new ByteArrayOutputStream();
    ByteArrayOutputStream dataErr= new ByteArrayOutputStream();
    System.setOut(new PrintStream(dataOut));
    System.setErr(new PrintStream(dataErr));


    try {
      String[] params = {"--verbose","file://" + template.getAbsolutePath() };

      assertEquals(0, fileCat.run(params));
      assertTrue(dataOut.toString().contains("123\t456\t789\t1000\t5.3\thive and hadoop\t\tNULL"));
      assertTrue(dataOut.toString().contains("100\t200\t123\t1000\t5.3\thive and hadoop\t\tNULL"));
      assertTrue(dataOut.toString().contains("200\t400\t678\t1000\t4.8\thive and hadoop\t\tTEST"));
      dataOut.reset();
       params = new String[] { "--start=-10","--file-sizes","file://" + template.getAbsolutePath() };
      assertEquals(0, fileCat.run(params));
      assertTrue(dataOut.toString().contains("File size (uncompressed): 105. File size (compressed): 134. Number of rows: 3."));
      dataOut.reset();

      params = new String[] {"--start=0", "--column-sizes","file://" + template.getAbsolutePath() };
      assertEquals(0, fileCat.run(params));
      assertTrue(dataOut.toString().contains("0\t9\t17"));
      assertTrue(dataOut.toString().contains("1\t9\t17"));
      assertTrue(dataOut.toString().contains("2\t9\t17"));
      assertTrue(dataOut.toString().contains("3\t12\t14"));
      assertTrue(dataOut.toString().contains("4\t9\t17"));
      assertTrue(dataOut.toString().contains("5\t45\t26"));


      dataOut.reset();
      params = new String[] {"--start=0", "--column-sizes-pretty","file://" + template.getAbsolutePath() };
      assertEquals(0, fileCat.run(params));
      assertTrue(dataOut.toString().contains("Column 0: Uncompressed size: 9 Compressed size: 17"));
      assertTrue(dataOut.toString().contains("Column 1: Uncompressed size: 9 Compressed size: 17"));
      assertTrue(dataOut.toString().contains("Column 2: Uncompressed size: 9 Compressed size: 17"));
      assertTrue(dataOut.toString().contains("Column 3: Uncompressed size: 12 Compressed size: 14"));
      assertTrue(dataOut.toString().contains("Column 4: Uncompressed size: 9 Compressed size: 17"));
      assertTrue(dataOut.toString().contains("Column 5: Uncompressed size: 45 Compressed size: 26"));

      params = new String[] { };
      assertEquals(-1, fileCat.run(params));
      assertTrue(dataErr.toString().contains("RCFileCat [--start=start_offet] [--length=len] [--verbose] " +
          "[--column-sizes | --column-sizes-pretty] [--file-sizes] fileName"));

      dataErr.reset();
      params = new String[] { "--fakeParameter","file://" + template.getAbsolutePath()};
      assertEquals(-1, fileCat.run(params));
      assertTrue(dataErr.toString().contains("RCFileCat [--start=start_offet] [--length=len] [--verbose] " +
          "[--column-sizes | --column-sizes-pretty] [--file-sizes] fileName"));

    } finally {
      // restore  input and output streams
      System.setOut(oldOutPrintStream);
      System.setErr(oldErrPrintStream);
    }

  }

  private void write(RCFile.Writer writer, byte[][] record) throws IOException {
    BytesRefArrayWritable bytes = new BytesRefArrayWritable(record.length);
    for (int i = 0; i < record.length; i++) {
      BytesRefWritable cu = new BytesRefWritable(record[i], 0, record[i].length);
      bytes.set(i, cu);
    }
    writer.append(bytes);

  }
}
