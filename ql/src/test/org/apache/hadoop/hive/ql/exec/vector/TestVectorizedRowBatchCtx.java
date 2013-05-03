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

package org.apache.hadoop.hive.ql.exec.vector;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.junit.Before;
import org.junit.Test;

/**
 * Class that tests the functionality of VectorizedRowBatchCtx
 */
public class TestVectorizedRowBatchCtx {

  private Configuration conf;
  private FileSystem fs;
  private Path testFilePath;
  private int colCount;
  private ColumnarSerDe serDe;
  private Properties tbl;

  @Before
  public void openFileSystem() throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    Path workDir = new Path(System.getProperty("test.tmp.dir",
        "target" + File.separator + "test" + File.separator + "tmp"));
    fs.setWorkingDirectory(workDir);
    testFilePath = new Path("TestVectorizedRowBatchCtx.testDump.rc");
    fs.delete(testFilePath, false);
  }

  private void InitSerde() {
    tbl = new Properties();

    // Set the configuration parameters
    tbl.setProperty(serdeConstants.SERIALIZATION_FORMAT, "6");
    tbl.setProperty("columns",
        "ashort,aint,along,adouble,afloat,astring");
    tbl.setProperty("columns.types",
        "smallint:int:bigint:double:float:string");
    colCount = 6;
    tbl.setProperty(serdeConstants.SERIALIZATION_NULL_FORMAT, "NULL");

    try {
      serDe = new ColumnarSerDe();
      serDe.initialize(conf, tbl);
    } catch (SerDeException e) {
      new RuntimeException(e);
    }
  }

  private void WriteRCFile(FileSystem fs, Path file, Configuration conf)
      throws IOException, SerDeException {
    fs.delete(file, true);

    RCFileOutputFormat.setColumnNumber(conf, colCount);
    RCFile.Writer writer =
        new RCFile.Writer(fs, conf, file, null, null,
            new DefaultCodec());

    for (int i = 0; i < 10; ++i) {
      BytesRefArrayWritable bytes = new BytesRefArrayWritable(colCount);
      BytesRefWritable cu;

      if (i % 3 != 0) {
        cu = new BytesRefWritable((i + "").getBytes("UTF-8"), 0, (i + "").getBytes("UTF-8").length);
        bytes.set(0, cu);

        cu = new BytesRefWritable((i + 100 + "").getBytes("UTF-8"), 0,
            (i + 100 + "").getBytes("UTF-8").length);
        bytes.set(1, cu);

        cu = new BytesRefWritable((i + 200 + "").getBytes("UTF-8"), 0,
            (i + 200 + "").getBytes("UTF-8").length);
        bytes.set(2, cu);

        cu = new BytesRefWritable((i + 1.23 + "").getBytes("UTF-8"), 0,
            (i + 1.23 + "").getBytes("UTF-8").length);
        bytes.set(3, cu);

        cu = new BytesRefWritable((i + 2.23 + "").getBytes("UTF-8"), 0,
            (i + 2.23 + "").getBytes("UTF-8").length);
        bytes.set(4, cu);

        cu = new BytesRefWritable(("Test string").getBytes("UTF-8"), 0,
            ("Test string").getBytes("UTF-8").length);
        bytes.set(5, cu);
      } else {
        cu = new BytesRefWritable((i + "").getBytes("UTF-8"), 0, (i + "").getBytes("UTF-8").length);
        bytes.set(0, cu);

        cu = new BytesRefWritable(new byte[0], 0, 0);
        bytes.set(1, cu);

        cu = new BytesRefWritable(new byte[0], 0, 0);
        bytes.set(2, cu);

        cu = new BytesRefWritable(new byte[0], 0, 0);
        bytes.set(3, cu);

        cu = new BytesRefWritable(new byte[0], 0, 0);
        bytes.set(4, cu);

        cu = new BytesRefWritable(("Test string").getBytes("UTF-8"), 0,
            ("NULL").getBytes("UTF-8").length);
        bytes.set(5, cu);
      }
      writer.append(bytes);
    }
    writer.close();
  }

  private VectorizedRowBatch GetRowBatch() throws SerDeException, HiveException, IOException {

    RCFile.Reader reader = new RCFile.Reader(fs, this.testFilePath, conf);

    // Get object inspector
    StructObjectInspector oi = (StructObjectInspector) serDe
        .getObjectInspector();
    List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();

    Assert.assertEquals("Field size should be 6", colCount, fieldRefs.size());

    // Create the context
    VectorizedRowBatchCtx ctx = new VectorizedRowBatchCtx(oi, oi, serDe, null);
    VectorizedRowBatch batch = ctx.CreateVectorizedRowBatch();
    ctx.SetNoNullFields(true, batch);

    // Iterate thru the rows and populate the batch
    LongWritable rowID = new LongWritable();
    for (int i = 0; i < 10; i++) {
      reader.next(rowID);
      BytesRefArrayWritable cols = new BytesRefArrayWritable();
      reader.getCurrentRow(cols);
      cols.resetValid(colCount);
      ctx.AddRowToBatch(i, cols, batch);
    }
    reader.close();
    batch.size = 10;
    return batch;
  }

  void ValidateRowBatch(VectorizedRowBatch batch) throws IOException, SerDeException {

    LongWritable rowID = new LongWritable();
    RCFile.Reader reader = new RCFile.Reader(fs, this.testFilePath, conf);
    for (int i = 0; i < batch.size; i++) {
      reader.next(rowID);
      BytesRefArrayWritable cols = new BytesRefArrayWritable();
      reader.getCurrentRow(cols);
      cols.resetValid(colCount);
      Object row = serDe.deserialize(cols);

      StructObjectInspector oi = (StructObjectInspector) serDe
          .getObjectInspector();
      List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();

      for (int j = 0; j < fieldRefs.size(); j++) {
        Object fieldData = oi.getStructFieldData(row, fieldRefs.get(j));
        ObjectInspector foi = fieldRefs.get(j).getFieldObjectInspector();

        // Vectorization only supports PRIMITIVE data types. Assert the same
        Assert.assertEquals(true, foi.getCategory() == Category.PRIMITIVE);

        PrimitiveObjectInspector poi = (PrimitiveObjectInspector) foi;
        Object writableCol = poi.getPrimitiveWritableObject(fieldData);
        if (writableCol != null) {
          switch (poi.getPrimitiveCategory()) {
          case SHORT: {
            LongColumnVector lcv = (LongColumnVector) batch.cols[j];
            Assert.assertEquals(true, lcv.vector[i] == ((ShortWritable) writableCol).get());
          }
            break;
          case INT: {
            LongColumnVector lcv = (LongColumnVector) batch.cols[j];
            Assert.assertEquals(true, lcv.vector[i] == ((IntWritable) writableCol).get());
          }
            break;
          case LONG: {
            LongColumnVector lcv = (LongColumnVector) batch.cols[j];
            Assert.assertEquals(true, lcv.vector[i] == ((LongWritable) writableCol).get());
          }
            break;
          case FLOAT: {
            DoubleColumnVector dcv = (DoubleColumnVector) batch.cols[j];
            Assert.assertEquals(true, dcv.vector[i] == ((FloatWritable) writableCol).get());
          }
            break;
          case DOUBLE: {
            DoubleColumnVector dcv = (DoubleColumnVector) batch.cols[j];
            Assert.assertEquals(true, dcv.vector[i] == ((DoubleWritable) writableCol).get());
          }
            break;
          case STRING: {
            BytesColumnVector bcv = (BytesColumnVector) batch.cols[j];
            Text colText = (Text) writableCol;
            Text batchText = (Text) bcv.getWritableObject(i);
            String a = colText.toString();
            String b = batchText.toString();
            Assert.assertEquals(true, a.equals(b));
          }
            break;
          default:
            Assert.assertEquals("Unknown type", false);
          }
        } else {
          Assert.assertEquals(true, batch.cols[j].isNull[i]);
        }
      }

      // Check repeating
      Assert.assertEquals(false, batch.cols[0].isRepeating);
      Assert.assertEquals(false, batch.cols[1].isRepeating);
      Assert.assertEquals(false, batch.cols[2].isRepeating);
      Assert.assertEquals(false, batch.cols[3].isRepeating);
      Assert.assertEquals(false, batch.cols[4].isRepeating);

      // Check non null
      Assert.assertEquals(true, batch.cols[0].noNulls);
      Assert.assertEquals(false, batch.cols[1].noNulls);
      Assert.assertEquals(false, batch.cols[2].noNulls);
      Assert.assertEquals(false, batch.cols[3].noNulls);
      Assert.assertEquals(false, batch.cols[4].noNulls);
    }
    reader.close();
  }

  @Test
  public void TestCtx() throws Exception {
      InitSerde();
      WriteRCFile(this.fs, this.testFilePath, this.conf);
      VectorizedRowBatch batch = GetRowBatch();
      ValidateRowBatch(batch);
  }
}
