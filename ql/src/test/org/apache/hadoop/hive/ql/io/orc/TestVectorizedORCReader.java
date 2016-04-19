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
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Random;
import org.apache.hadoop.io.Text;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

  @SuppressWarnings("unused")
  static class MyRecord {
    private final Boolean bo;
    private final Byte by;
    private final Integer i;
    private final Long l;
    private final Short s;
    private final Double d;
    private final String k;
    private final Timestamp t;
    private final Date dt;
    private final HiveDecimal hd;

    MyRecord(Boolean bo, Byte by, Integer i, Long l, Short s, Double d, String k,
        Timestamp t, Date dt, HiveDecimal hd) {
      this.bo = bo;
      this.by = by;
      this.i = i;
      this.l = l;
      this.s = s;
      this.d = d;
      this.k = k;
      this.t = t;
      this.dt = dt;
      this.hd = hd;
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
    String[] dates = new String[] {"1991-02-28", "1970-01-31", "1950-04-23"};
    String[] decimalStrings = new String[] {"234.443", "10001000", "0.3333367", "67788798.0", "-234.443",
        "-10001000", "-0.3333367", "-67788798.0", "0"};
    for (int i = 0; i < 21000; ++i) {
      if ((i % 7) != 0) {
        writer.addRow(new MyRecord(((i % 3) == 0), (byte)(i % 5), i, (long) 200, (short) (300 + i), (double) (400 + i),
            words[r1.nextInt(words.length)], new Timestamp(Calendar.getInstance().getTime().getTime()),
            Date.valueOf(dates[i % 3]), HiveDecimal.create(decimalStrings[i % decimalStrings.length])));
      } else {
        writer.addRow(new MyRecord(null, null, i, (long) 200, null, null, null, null, null, null));
      }
    }
    writer.close();
    checkVectorizedReader();
  }

  private void checkVectorizedReader() throws Exception {

    Reader vreader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf));
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf));
    RecordReaderImpl vrr = (RecordReaderImpl) vreader.rows();
    RecordReaderImpl rr = (RecordReaderImpl) reader.rows();
    VectorizedRowBatch batch = null;
    OrcStruct row = null;

    // Check Vectorized ORC reader against ORC row reader
    while (vrr.hasNext()) {
      batch = vrr.nextBatch(batch);
      for (int i = 0; i < batch.size; i++) {
        row = (OrcStruct) rr.next(row);
        for (int j = 0; j < batch.cols.length; j++) {
          Object a = (row.getFieldValue(j));
          ColumnVector cv = batch.cols[j];
          // if the value is repeating, use row 0
          int rowId = cv.isRepeating ? 0 : i;

          // make sure the null flag agrees
          if (a == null) {
            Assert.assertEquals(true, !cv.noNulls && cv.isNull[rowId]);
          } else if (a instanceof BooleanWritable) {

            // Boolean values are stores a 1's and 0's, so convert and compare
            Long temp = (long) (((BooleanWritable) a).get() ? 1 : 0);
            long b = ((LongColumnVector) cv).vector[rowId];
            Assert.assertEquals(temp.toString(), Long.toString(b));
          } else if (a instanceof TimestampWritable) {
            // Timestamps are stored as long, so convert and compare
            TimestampWritable t = ((TimestampWritable) a);
            TimestampColumnVector tcv = ((TimestampColumnVector) cv);
            Assert.assertEquals(t.getTimestamp(), tcv.asScratchTimestamp(rowId));

          } else if (a instanceof DateWritable) {
            // Dates are stored as long, so convert and compare

            DateWritable adt = (DateWritable) a;
            long b = ((LongColumnVector) cv).vector[rowId];
            Assert.assertEquals(adt.get().getTime(),
                DateWritable.daysToMillis((int) b));

          } else if (a instanceof HiveDecimalWritable) {
            // Decimals are stored as BigInteger, so convert and compare
            HiveDecimalWritable dec = (HiveDecimalWritable) a;
            HiveDecimalWritable b = ((DecimalColumnVector) cv).vector[i];
            Assert.assertEquals(dec, b);

          } else if (a instanceof DoubleWritable) {

            double b = ((DoubleColumnVector) cv).vector[rowId];
            assertEquals(a.toString(), Double.toString(b));
          } else if (a instanceof Text) {
            BytesColumnVector bcv = (BytesColumnVector) cv;
            Text b = new Text();
            b.set(bcv.vector[rowId], bcv.start[rowId], bcv.length[rowId]);
            assertEquals(a, b);
          } else if (a instanceof IntWritable ||
              a instanceof LongWritable ||
              a instanceof ByteWritable ||
              a instanceof ShortWritable) {
            assertEquals(a.toString(),
                Long.toString(((LongColumnVector) cv).vector[rowId]));
          } else {
            assertEquals("huh", a.getClass().getName());
          }
        }
      }

      // Check repeating
      Assert.assertEquals(false, batch.cols[0].isRepeating);
      Assert.assertEquals(false, batch.cols[1].isRepeating);
      Assert.assertEquals(false, batch.cols[2].isRepeating);
      Assert.assertEquals(true, batch.cols[3].isRepeating);
      Assert.assertEquals(false, batch.cols[4].isRepeating);
      Assert.assertEquals(false, batch.cols[5].isRepeating);
      Assert.assertEquals(false, batch.cols[6].isRepeating);
      Assert.assertEquals(false, batch.cols[7].isRepeating);
      Assert.assertEquals(false, batch.cols[8].isRepeating);
      Assert.assertEquals(false, batch.cols[9].isRepeating);

      // Check non null
      Assert.assertEquals(false, batch.cols[0].noNulls);
      Assert.assertEquals(false, batch.cols[1].noNulls);
      Assert.assertEquals(true, batch.cols[2].noNulls);
      Assert.assertEquals(true, batch.cols[3].noNulls);
      Assert.assertEquals(false, batch.cols[4].noNulls);
      Assert.assertEquals(false, batch.cols[5].noNulls);
      Assert.assertEquals(false, batch.cols[6].noNulls);
      Assert.assertEquals(false, batch.cols[7].noNulls);
      Assert.assertEquals(false, batch.cols[8].noNulls);
      Assert.assertEquals(false, batch.cols[9].noNulls);
    }
    Assert.assertEquals(false, rr.hasNext());
  }
}
