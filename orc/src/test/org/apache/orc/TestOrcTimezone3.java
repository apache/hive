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

import static junit.framework.Assert.assertEquals;

import java.io.File;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.Lists;

import junit.framework.Assert;

/**
 *
 */
@RunWith(Parameterized.class)
public class TestOrcTimezone3 {
  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));
  Configuration conf;
  FileSystem fs;
  Path testFilePath;
  String writerTimeZone;
  String readerTimeZone;
  static TimeZone defaultTimeZone = TimeZone.getDefault();

  public TestOrcTimezone3(String writerTZ, String readerTZ) {
    this.writerTimeZone = writerTZ;
    this.readerTimeZone = readerTZ;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    List<Object[]> result = Arrays.asList(new Object[][]{
        {"America/Chicago", "America/Los_Angeles"},
    });
    return result;
  }

  @Rule
  public TestName testCaseName = new TestName();

  @Before
  public void openFileSystem() throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestOrcTimezone3." +
        testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @After
  public void restoreTimeZone() {
    TimeZone.setDefault(defaultTimeZone);
  }

  @Test
  public void testTimestampWriter() throws Exception {
    TypeDescription schema = TypeDescription.createTimestamp();

    TimeZone.setDefault(TimeZone.getTimeZone(writerTimeZone));
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
            .bufferSize(10000));
    assertEquals(writerTimeZone, TimeZone.getDefault().getID());
    List<String> ts = Lists.newArrayList();
    ts.add("1969-12-31 16:00:14.007");
    ts.add("1969-12-31 16:00:06.021");
    ts.add("1969-12-31 16:00:03.963");
    VectorizedRowBatch batch = schema.createRowBatch();
    TimestampColumnVector times = (TimestampColumnVector) batch.cols[0];
    for (String t : ts) {
      times.set(batch.size++, Timestamp.valueOf(t));
    }
    writer.addRowBatch(batch);
    writer.close();

    TimeZone.setDefault(TimeZone.getTimeZone(readerTimeZone));
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(readerTimeZone, TimeZone.getDefault().getID());
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    times = (TimestampColumnVector) batch.cols[0];
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(ts.get(idx++), times.asScratchTimestamp(r).toString());
      }
    }
    rows.close();
  }
}
