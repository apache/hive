/*
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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import static org.apache.hadoop.hive.ql.io.orc.TestInputOutputFormat.createMockExecutionEnvironment;
import static org.apache.hadoop.hive.ql.io.orc.TestInputOutputFormat.setBlocks;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestRandomAccessHiveInputFormat {

  Path workDir = new Path(System.getProperty("test.tmp.dir","target/tmp"));

  /**
   * MockFileSystem that pretends to be an S3A system
   */
  public static class MockS3aFileSystem
      extends TestInputOutputFormat.MockFileSystem {

    @Override
    public String getScheme() {
      return "s3a";
    }
  }

  @Test
  // Make sure that the FS InputPolicy is changed to Random for ORC on S3A
  public void testOrcSplitOnS3A() throws Exception {
    // get the object inspector for MyRow
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(TestInputOutputFormat.MyRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    // Use ORC files stored on S3A
    JobConf conf = createMockExecutionEnvironment(workDir, new Path("mock:///"),
        "randomAccessVectorized", inspector, true, 1, MockS3aFileSystem.class.getName());

    // write the orc file to the mock file system
    Path path = new Path(conf.get("mapred.input.dir") + "/0_0");
    Writer writer = OrcFile.createWriter(path, OrcFile.writerOptions(conf).blockPadding(false)
        .bufferSize(1024).inspector(inspector));
    writer.addRow(new TestInputOutputFormat.MyRow(0, 0));
    writer.addRow(new TestInputOutputFormat.MyRow(1, 2));
    writer.close();

    setBlocks(path, conf, new TestInputOutputFormat.MockBlock("host0"));

    HiveInputFormat<WritableComparable, Writable> inputFormat = new HiveInputFormat<>();

    InputSplit[] splits = inputFormat.getSplits(conf, 2);
    assertEquals(1, splits.length);

    Throwable thrown = null;
    try {
      inputFormat.getRecordReader(splits[0], conf, Reporter.NULL);
    } catch (Exception e) {
      thrown = e;
    }

    // As we are mocking a simple FS we just expect an cast exception to occur
    assertEquals(thrown.getClass(), ClassCastException.class);
    assertTrue(thrown.getMessage().contains("S3AFileSystem"));
  }
}
