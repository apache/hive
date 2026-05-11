/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.orc;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class TestOrcFileStripeMergeRecordReader {

  private static final int MAX_ROWS_PER_STRIPE = 5000;

  private static final long STRIPE_SIZE_BYTES = 128;

  private OrcFileKeyWrapper key;
  private OrcFileValueWrapper value;
  private Path tmpPath;
  private Configuration conf;
  private FileSystem fs;

  @Rule
  public TestName testCaseName = new TestName();

  @Before
  public void setup() throws IOException {
    conf = new Configuration();
    // ORC ≥2.x: orc.stripe.size.check.ratio triggers flushes when buffered tree bytes exceed ratio × orc.stripe.size
    // Setting it to 0 disables it.
    OrcConf.STRIPE_SIZE_CHECKRATIO.setDouble(conf, 0);
    // Maximum number of rows a Stripe can hold in ORC file.
    OrcConf.STRIPE_ROW_COUNT.setLong(conf, MAX_ROWS_PER_STRIPE);
    fs = FileSystem.getLocal(conf);
    key = new OrcFileKeyWrapper();
    value = new OrcFileValueWrapper();
    tmpPath  = prepareTmpPath();
    createOrcFile(MAX_ROWS_PER_STRIPE + 1);
  }

  @After
  public void tearDown() throws IOException {
    fs.delete(tmpPath, false);
  }

  @Test
  public void testSplitStartsWithNonZeroOffset() throws IOException {
    FileStatus fileStatus = fs.getFileStatus(tmpPath);
    long length = fileStatus.getLen();
    long offset = length / 2;

    // Check case for non-zero offset, the file will be skipped.
    FileSplit split = new FileSplit(tmpPath, offset, length, (String[]) null);
    OrcFileStripeMergeRecordReader reader = new OrcFileStripeMergeRecordReader(conf, split);
    reader.next(key, value);
    Assert.assertNull(key.getInputPath());
  }

  @Test
  public void testSplitStartsWithZeroOffset() throws IOException {
    FileStatus fileStatus = fs.getFileStatus(tmpPath);
    long length = fileStatus.getLen();
    // New split with zero offset, the file should be processed.
    FileSplit split = new FileSplit(tmpPath, 0, length, (String[]) null);
    OrcFileStripeMergeRecordReader reader = new OrcFileStripeMergeRecordReader(conf, split);
    // both stripes will be processed, first stripe has 5000 rows and second stripe has 1 row
    reader.next(key, value);
    Assert.assertEquals("InputPath", tmpPath, key.getInputPath());
    Assert.assertEquals("NumberOfValues", MAX_ROWS_PER_STRIPE,
        value.getStripeStatistics().getColStats(0).getNumberOfValues());
    reader.next(key, value);
    Assert.assertEquals("InputPath", tmpPath, key.getInputPath());
    Assert.assertEquals("NumberOfValues", 1L, value.getStripeStatistics().getColStats(0).getNumberOfValues());
    // we are done with the file, so expect null path
    Assert.assertFalse(reader.next(key, value));
    reader.close();
  }

  private void createOrcFile(int numberOfRows) throws IOException {
    ObjectInspector inspector;
    synchronized (TestOrcFileStripeMergeRecordReader.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (StringIntIntIntRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    Writer writer = OrcFile.createWriter(tmpPath,
        OrcFile.writerOptions(conf)
            .inspector(inspector)
            .stripeSize(STRIPE_SIZE_BYTES)
            .compress(CompressionKind.ZLIB)
            .bufferSize(5000)
            .rowIndexStride(1000));

    Random rand = new Random(157);

    for (int i = 0; i < numberOfRows; i++) {
      writer.addRow(new StringIntIntIntRow(
          Integer.toBinaryString(i),
          rand.nextInt(),
          rand.nextInt(),
          rand.nextInt()
      ));
    }
    writer.close();
  }

  private Path prepareTmpPath() throws IOException {
    Path path = new Path(System.getProperty("test.tmp.dir",
        "target" + File.separator + "test" + File.separator + "tmp")
        + File.separator + "TestOrcFile." + testCaseName.getMethodName() + ".orc");
    fs.delete(path, false);
    return path;
  }

  public static class StringIntIntIntRow {

    Text string1 = new Text();
    int int1;
    int int2;
    int int3;

    StringIntIntIntRow(String string1, int int1, int int2, int int3) {
      this.string1.set(string1);
      this.int1 = int1;
      this.int2 = int2;
      this.int3 = int3;
    }
  }
}
