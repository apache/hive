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

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class TestOrcWideTable {

  private static final int MEMORY_FOR_ORC = 512 * 1024 * 1024;
  Path workDir = new Path(System.getProperty("test.tmp.dir", "target" + File.separator + "test"
      + File.separator + "tmp"));

  Configuration conf;
  FileSystem fs;
  Path testFilePath;
  float memoryPercent;

  @Rule
  public TestName testCaseName = new TestName();

  @Before
  public void openFileSystem() throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestOrcFile." + testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
    // make sure constant memory is available for ORC always
    memoryPercent = (float) MEMORY_FOR_ORC / (float) ManagementFactory.getMemoryMXBean().
        getHeapMemoryUsage().getMax();
    conf.setFloat(HiveConf.ConfVars.HIVE_ORC_FILE_MEMORY_POOL.varname, memoryPercent);
  }

  @Test
  public void testBufferSizeFor1Col() throws IOException {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(Long.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    int bufferSize = 128 * 1024;
    Writer writer = OrcFile.createWriter(
        testFilePath,
        OrcFile.writerOptions(conf).inspector(inspector).stripeSize(100000)
            .compress(CompressionKind.NONE).bufferSize(bufferSize));
    final int newBufferSize;
    if (writer instanceof WriterImpl) {
      WriterImpl orcWriter = (WriterImpl) writer;
      newBufferSize = orcWriter.getEstimatedBufferSize(bufferSize);
      assertEquals(bufferSize, newBufferSize);
    }
  }

  @Test
  public void testBufferSizeFor1000Col() throws IOException {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(Long.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    int bufferSize = 128 * 1024;
    String columns = getRandomColumnNames(1000);
    // just for testing. manually write the column names
    conf.set(IOConstants.COLUMNS, columns);
    Writer writer = OrcFile.createWriter(
        testFilePath,
        OrcFile.writerOptions(conf).inspector(inspector).stripeSize(100000)
            .compress(CompressionKind.NONE).bufferSize(bufferSize));
    final int newBufferSize;
    if (writer instanceof WriterImpl) {
      WriterImpl orcWriter = (WriterImpl) writer;
      newBufferSize = orcWriter.getEstimatedBufferSize(bufferSize);
      assertEquals(bufferSize, newBufferSize);
    }
  }

  @Test
  public void testBufferSizeFor2000Col() throws IOException {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(Long.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    int bufferSize = 256 * 1024;
    String columns = getRandomColumnNames(2000);
    // just for testing. manually write the column names
    conf.set(IOConstants.COLUMNS, columns);
    Writer writer = OrcFile.createWriter(
        testFilePath,
        OrcFile.writerOptions(conf).inspector(inspector).stripeSize(100000)
            .compress(CompressionKind.ZLIB).bufferSize(bufferSize));
    final int newBufferSize;
    if (writer instanceof WriterImpl) {
      WriterImpl orcWriter = (WriterImpl) writer;
      newBufferSize = orcWriter.getEstimatedBufferSize(bufferSize);
      assertEquals(32 * 1024, newBufferSize);
    }
  }

  @Test
  public void testBufferSizeFor2000ColNoCompression() throws IOException {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(Long.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    int bufferSize = 256 * 1024;
    String columns = getRandomColumnNames(2000);
    // just for testing. manually write the column names
    conf.set(IOConstants.COLUMNS, columns);
    Writer writer = OrcFile.createWriter(
        testFilePath,
        OrcFile.writerOptions(conf).inspector(inspector).stripeSize(100000)
            .compress(CompressionKind.NONE).bufferSize(bufferSize));
    final int newBufferSize;
    if (writer instanceof WriterImpl) {
      WriterImpl orcWriter = (WriterImpl) writer;
      newBufferSize = orcWriter.getEstimatedBufferSize(bufferSize);
      assertEquals(64 * 1024, newBufferSize);
    }
  }

  @Test
  public void testBufferSizeFor4000Col() throws IOException {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(Long.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    int bufferSize = 256 * 1024;
    String columns = getRandomColumnNames(4000);
    // just for testing. manually write the column names
    conf.set(IOConstants.COLUMNS, columns);
    Writer writer = OrcFile.createWriter(
        testFilePath,
        OrcFile.writerOptions(conf).inspector(inspector).stripeSize(100000)
            .compress(CompressionKind.ZLIB).bufferSize(bufferSize));
    final int newBufferSize;
    if (writer instanceof WriterImpl) {
      WriterImpl orcWriter = (WriterImpl) writer;
      newBufferSize = orcWriter.getEstimatedBufferSize(bufferSize);
      assertEquals(16 * 1024, newBufferSize);
    }
  }

  @Test
  public void testBufferSizeFor4000ColNoCompression() throws IOException {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(Long.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    int bufferSize = 256 * 1024;
    String columns = getRandomColumnNames(4000);
    // just for testing. manually write the column names
    conf.set(IOConstants.COLUMNS, columns);
    Writer writer = OrcFile.createWriter(
        testFilePath,
        OrcFile.writerOptions(conf).inspector(inspector).stripeSize(100000)
            .compress(CompressionKind.NONE).bufferSize(bufferSize));
    final int newBufferSize;
    if (writer instanceof WriterImpl) {
      WriterImpl orcWriter = (WriterImpl) writer;
      newBufferSize = orcWriter.getEstimatedBufferSize(bufferSize);
      assertEquals(32 * 1024, newBufferSize);
    }
  }

  @Test
  public void testBufferSizeFor25000Col() throws IOException {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(Long.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    int bufferSize = 256 * 1024;
    String columns = getRandomColumnNames(25000);
    // just for testing. manually write the column names
    conf.set(IOConstants.COLUMNS, columns);
    Writer writer = OrcFile.createWriter(
        testFilePath,
        OrcFile.writerOptions(conf).inspector(inspector).stripeSize(100000)
            .compress(CompressionKind.NONE).bufferSize(bufferSize));
    final int newBufferSize;
    if (writer instanceof WriterImpl) {
      WriterImpl orcWriter = (WriterImpl) writer;
      newBufferSize = orcWriter.getEstimatedBufferSize(bufferSize);
      // 4K is the minimum buffer size
      assertEquals(4 * 1024, newBufferSize);
    }
  }

  @Test
  public void testBufferSizeManualOverride1() throws IOException {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(Long.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    int bufferSize = 1024;
    String columns = getRandomColumnNames(2000);
    // just for testing. manually write the column names
    conf.set(IOConstants.COLUMNS, columns);
    Writer writer = OrcFile.createWriter(
        testFilePath,
        OrcFile.writerOptions(conf).inspector(inspector).stripeSize(100000)
            .compress(CompressionKind.NONE).bufferSize(bufferSize));
    final int newBufferSize;
    if (writer instanceof WriterImpl) {
      WriterImpl orcWriter = (WriterImpl) writer;
      newBufferSize = orcWriter.getEstimatedBufferSize(bufferSize);
      assertEquals(bufferSize, newBufferSize);
    }
  }

  @Test
  public void testBufferSizeManualOverride2() throws IOException {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(Long.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    int bufferSize = 2 * 1024;
    String columns = getRandomColumnNames(4000);
    // just for testing. manually write the column names
    conf.set(IOConstants.COLUMNS, columns);
    Writer writer = OrcFile.createWriter(
        testFilePath,
        OrcFile.writerOptions(conf).inspector(inspector).stripeSize(100000)
            .compress(CompressionKind.NONE).bufferSize(bufferSize));
    final int newBufferSize;
    if (writer instanceof WriterImpl) {
      WriterImpl orcWriter = (WriterImpl) writer;
      newBufferSize = orcWriter.getEstimatedBufferSize(bufferSize);
      assertEquals(bufferSize, newBufferSize);
    }
  }

  private String getRandomColumnNames(int n) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < n - 1; i++) {
      sb.append("col").append(i).append(",");
    }
    sb.append("col").append(n - 1);
    return sb.toString();
  }
}
