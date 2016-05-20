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
package org.apache.orc.impl;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.tools.FileDump;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class TestRLEv2 {
  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));
  Path testFilePath;
  Configuration conf;
  FileSystem fs;

  @Rule
  public TestName testCaseName = new TestName();

  @Before
  public void openFileSystem () throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestRLEv2." +
        testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }

  void appendInt(VectorizedRowBatch batch, int i) {
    ((LongColumnVector) batch.cols[0]).vector[batch.size++] = i;
  }

  @Test
  public void testFixedDeltaZero() throws Exception {
    TypeDescription schema = TypeDescription.createInt();
    Writer w = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .compress(CompressionKind.NONE)
            .setSchema(schema)
            .rowIndexStride(0)
            .encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION)
            .version(OrcFile.Version.V_0_12)
    );
    VectorizedRowBatch batch = schema.createRowBatch(5120);
    for (int i = 0; i < 5120; ++i) {
      appendInt(batch, 123);
    }
    w.addRowBatch(batch);
    w.close();

    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{testFilePath.toUri().toString()});
    System.out.flush();
    String outDump = new String(myOut.toByteArray());
    // 10 runs of 512 elements. Each run has 2 bytes header, 2 bytes base (base = 123,
    // zigzag encoded varint) and 1 byte delta (delta = 0). In total, 5 bytes per run.
    assertEquals(true, outDump.contains("Stream: column 0 section DATA start: 3 length 50"));
    System.setOut(origOut);
  }

  @Test
  public void testFixedDeltaOne() throws Exception {
    TypeDescription schema = TypeDescription.createInt();
    Writer w = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .compress(CompressionKind.NONE)
            .setSchema(schema)
            .rowIndexStride(0)
            .encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION)
            .version(OrcFile.Version.V_0_12)
    );
    VectorizedRowBatch batch = schema.createRowBatch(5120);
    for (int i = 0; i < 5120; ++i) {
      appendInt(batch, i % 512);
    }
    w.addRowBatch(batch);
    w.close();

    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{testFilePath.toUri().toString()});
    System.out.flush();
    String outDump = new String(myOut.toByteArray());
    // 10 runs of 512 elements. Each run has 2 bytes header, 1 byte base (base = 0)
    // and 1 byte delta (delta = 1). In total, 4 bytes per run.
    assertEquals(true, outDump.contains("Stream: column 0 section DATA start: 3 length 40"));
    System.setOut(origOut);
  }

  @Test
  public void testFixedDeltaOneDescending() throws Exception {
    TypeDescription schema = TypeDescription.createInt();
    Writer w = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .compress(CompressionKind.NONE)
            .setSchema(schema)
            .rowIndexStride(0)
            .encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION)
            .version(OrcFile.Version.V_0_12)
    );
    VectorizedRowBatch batch = schema.createRowBatch(5120);
    for (int i = 0; i < 5120; ++i) {
      appendInt(batch, 512 - (i % 512));
    }
    w.addRowBatch(batch);
    w.close();

    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{testFilePath.toUri().toString()});
    System.out.flush();
    String outDump = new String(myOut.toByteArray());
    // 10 runs of 512 elements. Each run has 2 bytes header, 2 byte base (base = 512, zigzag + varint)
    // and 1 byte delta (delta = 1). In total, 5 bytes per run.
    assertEquals(true, outDump.contains("Stream: column 0 section DATA start: 3 length 50"));
    System.setOut(origOut);
  }

  @Test
  public void testFixedDeltaLarge() throws Exception {
    TypeDescription schema = TypeDescription.createInt();
    Writer w = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .compress(CompressionKind.NONE)
            .setSchema(schema)
            .rowIndexStride(0)
            .encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION)
            .version(OrcFile.Version.V_0_12)
    );
    VectorizedRowBatch batch = schema.createRowBatch(5120);
    for (int i = 0; i < 5120; ++i) {
      appendInt(batch, i % 512 + ((i % 512) * 100));
    }
    w.addRowBatch(batch);
    w.close();

    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{testFilePath.toUri().toString()});
    System.out.flush();
    String outDump = new String(myOut.toByteArray());
    // 10 runs of 512 elements. Each run has 2 bytes header, 1 byte base (base = 0)
    // and 2 bytes delta (delta = 100, zigzag encoded varint). In total, 5 bytes per run.
    assertEquals(true, outDump.contains("Stream: column 0 section DATA start: 3 length 50"));
    System.setOut(origOut);
  }

  @Test
  public void testFixedDeltaLargeDescending() throws Exception {
    TypeDescription schema = TypeDescription.createInt();
    Writer w = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .compress(CompressionKind.NONE)
            .setSchema(schema)
            .rowIndexStride(0)
            .encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION)
            .version(OrcFile.Version.V_0_12)
    );
    VectorizedRowBatch batch = schema.createRowBatch(5120);
    for (int i = 0; i < 5120; ++i) {
      appendInt(batch, (512 - i % 512) + ((i % 512) * 100));
    }
    w.addRowBatch(batch);
    w.close();

    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{testFilePath.toUri().toString()});
    System.out.flush();
    String outDump = new String(myOut.toByteArray());
    // 10 runs of 512 elements. Each run has 2 bytes header, 2 byte base (base = 512, zigzag + varint)
    // and 2 bytes delta (delta = 100, zigzag encoded varint). In total, 6 bytes per run.
    assertEquals(true, outDump.contains("Stream: column 0 section DATA start: 3 length 60"));
    System.setOut(origOut);
  }

  @Test
  public void testShortRepeat() throws Exception {
    TypeDescription schema = TypeDescription.createInt();
    Writer w = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .compress(CompressionKind.NONE)
            .setSchema(schema)
            .rowIndexStride(0)
            .encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION)
            .version(OrcFile.Version.V_0_12)
    );
    VectorizedRowBatch batch = schema.createRowBatch(5120);
    for (int i = 0; i < 5; ++i) {
      appendInt(batch, 10);
    }
    w.addRowBatch(batch);
    w.close();

    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{testFilePath.toUri().toString()});
    System.out.flush();
    String outDump = new String(myOut.toByteArray());
    // 1 byte header + 1 byte value
    assertEquals(true, outDump.contains("Stream: column 0 section DATA start: 3 length 2"));
    System.setOut(origOut);
  }

  @Test
  public void testDeltaUnknownSign() throws Exception {
    TypeDescription schema = TypeDescription.createInt();
    Writer w = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .compress(CompressionKind.NONE)
            .setSchema(schema)
            .rowIndexStride(0)
            .encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION)
            .version(OrcFile.Version.V_0_12)
    );
    VectorizedRowBatch batch = schema.createRowBatch(5120);
    appendInt(batch, 0);
    for (int i = 0; i < 511; ++i) {
      appendInt(batch, i);
    }
    w.addRowBatch(batch);
    w.close();

    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{testFilePath.toUri().toString()});
    System.out.flush();
    String outDump = new String(myOut.toByteArray());
    // monotonicity will be undetermined for this sequence 0,0,1,2,3,...510. Hence DIRECT encoding
    // will be used. 2 bytes for header and 640 bytes for data (512 values with fixed bit of 10 bits
    // each, 5120/8 = 640). Total bytes 642
    assertEquals(true, outDump.contains("Stream: column 0 section DATA start: 3 length 642"));
    System.setOut(origOut);
  }

  @Test
  public void testPatchedBase() throws Exception {
    TypeDescription schema = TypeDescription.createInt();
    Writer w = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .compress(CompressionKind.NONE)
            .setSchema(schema)
            .rowIndexStride(0)
            .encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION)
            .version(OrcFile.Version.V_0_12)
    );

    Random rand = new Random(123);
    VectorizedRowBatch batch = schema.createRowBatch(5120);
    appendInt(batch, 10000000);
    for (int i = 0; i < 511; ++i) {
      appendInt(batch, rand.nextInt(i+1));
    }
    w.addRowBatch(batch);
    w.close();

    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{testFilePath.toUri().toString()});
    System.out.flush();
    String outDump = new String(myOut.toByteArray());
    // use PATCHED_BASE encoding
    assertEquals(true, outDump.contains("Stream: column 0 section DATA start: 3 length 583"));
    System.setOut(origOut);
  }
}
