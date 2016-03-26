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

package org.apache.orc;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;

@RunWith(value = Parameterized.class)
public class TestUnrolledBitPack {

  private long val;

  public TestUnrolledBitPack(long val) {
    this.val = val;
  }

  @Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] { { -1 }, { 1 }, { 7 }, { -128 }, { 32000 }, { 8300000 },
        { Integer.MAX_VALUE }, { 540000000000L }, { 140000000000000L }, { 36000000000000000L },
        { Long.MAX_VALUE } };
    return Arrays.asList(data);
  }

  Path workDir = new Path(System.getProperty("test.tmp.dir", "target" + File.separator + "test"
      + File.separator + "tmp"));

  Configuration conf;
  FileSystem fs;
  Path testFilePath;

  @Rule
  public TestName testCaseName = new TestName();

  @Before
  public void openFileSystem() throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestOrcFile." + testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @Test
  public void testBitPacking() throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    long[] inp = new long[] { val, 0, val, val, 0, val, 0, val, val, 0, val, 0, val, val, 0, 0,
        val, val, 0, val, 0, 0, val, 0, val, 0, val, 0, 0, val, 0, val, 0, val, 0, 0, val, 0, val,
        0, val, 0, 0, val, 0, val, 0, val, 0, 0, val, 0, val, 0, val, 0, 0, val, 0, val, 0, val, 0,
        0, val, 0, val, 0, val, 0, 0, val, 0, val, 0, val, 0, 0, val, 0, val, 0, val, 0, 0, val, 0,
        val, 0, val, 0, 0, val, 0, val, 0, 0, val, val };
    List<Long> input = Lists.newArrayList(Longs.asList(inp));

    Writer writer = OrcFile.createWriter(
        testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
            .compress(CompressionKind.NONE).bufferSize(10000));
    VectorizedRowBatch batch = schema.createRowBatch();
    for (Long l : input) {
      int row = batch.size++;
      ((LongColumnVector) batch.cols[0]).vector[row] = l;
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

}
