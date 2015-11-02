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

package org.apache.hadoop.hive.ql.io.orc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hive.common.util.HiveTestUtils;
import org.apache.orc.CompressionKind;
import org.junit.Before;
import org.junit.Test;

public class TestJsonFileDump {

  Path workDir = new Path(System.getProperty("test.tmp.dir"));
  Configuration conf;
  FileSystem fs;
  Path testFilePath;

  @Before
  public void openFileSystem () throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    fs.setWorkingDirectory(workDir);
    testFilePath = new Path("TestFileDump.testDump.orc");
    fs.delete(testFilePath, false);
  }

  static class MyRecord {
    int i;
    long l;
    String s;
    MyRecord(int i, long l, String s) {
      this.i = i;
      this.l = l;
      this.s = s;
    }
  }

  static void checkOutput(String expected,
                                  String actual) throws Exception {
    BufferedReader eStream =
        new BufferedReader(new FileReader(HiveTestUtils.getFileFromClasspath(expected)));
    BufferedReader aStream =
        new BufferedReader(new FileReader(actual));
    String expectedLine = eStream.readLine();
    while (expectedLine != null) {
      String actualLine = aStream.readLine();
      System.out.println("actual:   " + actualLine);
      System.out.println("expected: " + expectedLine);
      assertEquals(expectedLine, actualLine);
      expectedLine = eStream.readLine();
    }
    assertNull(eStream.readLine());
    assertNull(aStream.readLine());
  }

  @Test
  public void testJsonDump() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (MyRecord.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    conf.set(HiveConf.ConfVars.HIVE_ORC_ENCODING_STRATEGY.varname, "COMPRESSION");
    OrcFile.WriterOptions options = OrcFile.writerOptions(conf)
        .fileSystem(fs)
        .inspector(inspector)
        .stripeSize(100000)
        .compress(CompressionKind.ZLIB)
        .bufferSize(10000)
        .rowIndexStride(1000)
        .bloomFilterColumns("s");
    Writer writer = OrcFile.createWriter(testFilePath, options);
    Random r1 = new Random(1);
    String[] words = new String[]{"It", "was", "the", "best", "of", "times,",
        "it", "was", "the", "worst", "of", "times,", "it", "was", "the", "age",
        "of", "wisdom,", "it", "was", "the", "age", "of", "foolishness,", "it",
        "was", "the", "epoch", "of", "belief,", "it", "was", "the", "epoch",
        "of", "incredulity,", "it", "was", "the", "season", "of", "Light,",
        "it", "was", "the", "season", "of", "Darkness,", "it", "was", "the",
        "spring", "of", "hope,", "it", "was", "the", "winter", "of", "despair,",
        "we", "had", "everything", "before", "us,", "we", "had", "nothing",
        "before", "us,", "we", "were", "all", "going", "direct", "to",
        "Heaven,", "we", "were", "all", "going", "direct", "the", "other",
        "way"};
    for(int i=0; i < 21000; ++i) {
      if (i % 100 == 0) {
        writer.addRow(new MyRecord(r1.nextInt(), r1.nextLong(), null));
      } else {
        writer.addRow(new MyRecord(r1.nextInt(), r1.nextLong(),
            words[r1.nextInt(words.length)]));
      }
    }

    writer.close();
    PrintStream origOut = System.out;
    String outputFilename = "orc-file-dump.json";
    FileOutputStream myOut = new FileOutputStream(workDir + File.separator + outputFilename);

    // replace stdout and run command
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{testFilePath.toString(), "-j", "-p", "--rowindex=3"});
    System.out.flush();
    System.setOut(origOut);


    checkOutput(outputFilename, workDir + File.separator + outputFilename);
  }
}
