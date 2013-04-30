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

import java.io.File;
import java.util.Random;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.Writable;
import org.junit.Before;
import org.junit.Test;

/**
*
* Class that tests ORC reader vectorization by comparing records that are
* returned by "row by row" reader with batch reader.
*
*/
public class TestVectorizedORCReader {

  private Configuration conf;
  private FileSystem fs;
  private Path testFilePath;

  @Before
  public void openFileSystem() throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    Path workDir = new Path(System.getProperty("test.tmp.dir",
        "target" + File.separator + "test" + File.separator + "tmp"));
    fs.setWorkingDirectory(workDir);
    testFilePath = new Path("TestVectorizedORCReader.testDump.orc");
    fs.delete(testFilePath, false);
  }

  static class MyRecord {
    private final Integer i;
    private final Long l;
    private final Short s;
    private final Double d;
    private final String k;

    MyRecord(Integer i, Long l, Short s, Double d, String k) {
      this.i = i;
      this.l = l;
      this.s = s;
      this.d = d;
      this.k = k;
    }
  }

  @Test
  public void createFile() throws Exception {
    ObjectInspector inspector;
    synchronized (TestVectorizedORCReader.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (MyRecord.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
        100000, CompressionKind.ZLIB, 10000, 10000);
    Random r1 = new Random(1);
    String[] words = new String[] {"It", "was", "the", "best", "of", "times,",
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
    for (int i = 0; i < 21000; ++i) {
      if ((i % 3) != 0) {
        writer.addRow(new MyRecord(i, (long) 200, (short) (300 + i), (double) (400 + i),
            words[r1.nextInt(words.length)]));
      } else {
        writer.addRow(new MyRecord(i, (long) 200, null, null, null));
      }
    }
    writer.close();
    checkVectorizedReader();
  }

  private void checkVectorizedReader() throws Exception {

    Reader vreader = OrcFile.createReader(testFilePath.getFileSystem(conf), testFilePath);
    Reader reader = OrcFile.createReader(testFilePath.getFileSystem(conf), testFilePath);
    RecordReaderImpl vrr = (RecordReaderImpl) vreader.rows(null);
    RecordReaderImpl rr = (RecordReaderImpl) reader.rows(null);
    VectorizedRowBatch batch = null;
    OrcStruct row = null;

    // Check Vectorized ORC reader against ORC row reader
    while (vrr.hasNext()) {
      batch = vrr.nextBatch(batch);
      for (int i = 0; i < batch.size; i++) {
        row = (OrcStruct) rr.next((Object) row);
        for (int j = 0; j < batch.cols.length; j++) {
          Object a = ((Writable) row.getFieldValue(j));
          Object b = batch.cols[j].getWritableObject(i);
          if (null == a) {
            Assert.assertEquals(true, (b == null));
          } else {
            Assert.assertEquals(true, b.toString().equals(a.toString()));
          }
        }
      }

      // Check repeating
      Assert.assertEquals(false, batch.cols[0].isRepeating);
      Assert.assertEquals(true, batch.cols[1].isRepeating);
      Assert.assertEquals(false, batch.cols[2].isRepeating);
      Assert.assertEquals(false, batch.cols[3].isRepeating);
      Assert.assertEquals(false, batch.cols[4].isRepeating);

      // Check non null
      Assert.assertEquals(true, batch.cols[0].noNulls);
      Assert.assertEquals(true, batch.cols[1].noNulls);
      Assert.assertEquals(false, batch.cols[2].noNulls);
      Assert.assertEquals(false, batch.cols[3].noNulls);
      Assert.assertEquals(false, batch.cols[4].noNulls);
    }
    Assert.assertEquals(false, rr.hasNext());
  }
}
