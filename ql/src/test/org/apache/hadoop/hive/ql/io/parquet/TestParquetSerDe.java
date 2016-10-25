/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet;

import java.util.Properties;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class TestParquetSerDe extends TestCase {

  public void testParquetHiveSerDe() throws Throwable {
    try {
      // Create the SerDe
      System.out.println("test: testParquetHiveSerDe");

      final ParquetHiveSerDe serDe = new ParquetHiveSerDe();
      final Configuration conf = new Configuration();
      final Properties tbl = createProperties();
      SerDeUtils.initializeSerDe(serDe, conf, tbl, null);

      // Data
      final Writable[] arr = new Writable[9];

      //primitive types
      arr[0] = new ByteWritable((byte) 123);
      arr[1] = new ShortWritable((short) 456);
      arr[2] = new IntWritable(789);
      arr[3] = new LongWritable(1000l);
      arr[4] = new DoubleWritable((double) 5.3);
      arr[5] = new BytesWritable("hive and hadoop and parquet. Big family.".getBytes("UTF-8"));
      arr[6] = new BytesWritable("parquetSerde binary".getBytes("UTF-8"));
      final Writable[] map = new Writable[3];
      for (int i = 0; i < 3; ++i) {
        final Writable[] pair = new Writable[2];
        pair[0] = new BytesWritable(("key_" + i).getBytes("UTF-8"));
        pair[1] = new IntWritable(i);
        map[i] = new ArrayWritable(Writable.class, pair);
      }
      arr[7] = new ArrayWritable(Writable.class, map);

      final Writable[] array = new Writable[5];
      for (int i = 0; i < 5; ++i) {
        array[i] = new BytesWritable(("elem_" + i).getBytes("UTF-8"));
      }
      arr[8] = new ArrayWritable(Writable.class, array);

      final ArrayWritable arrWritable = new ArrayWritable(Writable.class, arr);
      // Test
      deserializeAndSerializeLazySimple(serDe, arrWritable);
      System.out.println("test: testParquetHiveSerDe - OK");

    } catch (final Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void testParquetHiveSerDeComplexTypes() throws Throwable {
    // Initialize
    ParquetHiveSerDe serDe = new ParquetHiveSerDe();
    Configuration conf = new Configuration();
    Properties tblProperties = new Properties();

    tblProperties.setProperty(serdeConstants.LIST_COLUMNS, "a,s");
    tblProperties.setProperty(serdeConstants.LIST_COLUMN_TYPES, "int,struct<a:int,b:string>");
    conf.set(ColumnProjectionUtils.READ_NESTED_COLUMN_PATH_CONF_STR, "s.b");

    serDe.initialize(conf, tblProperties);

    // Generate test data
    Writable[] wb = new Writable[1];
    wb[0] = new BytesWritable("foo".getBytes("UTF-8"));
    Writable[] ws = new Writable[2];
    ws[0] = null;
    ArrayWritable awb = new ArrayWritable(Writable.class, wb);
    ws[1] = awb;
    ArrayWritable aws = new ArrayWritable(Writable.class, ws);

    // Inspect the test data
    StructObjectInspector soi = (StructObjectInspector) serDe.getObjectInspector();
    StructField s = soi.getStructFieldRef("s");
    assertEquals(awb, soi.getStructFieldData(aws, s));
    StructObjectInspector boi = (StructObjectInspector) s.getFieldObjectInspector();
    StructField b = boi.getStructFieldRef("b");
    assertEquals(wb[0], boi.getStructFieldData(awb, b));
  }

  private void deserializeAndSerializeLazySimple(final ParquetHiveSerDe serDe, final ArrayWritable t) throws SerDeException {

    // Get the row structure
    final StructObjectInspector oi = (StructObjectInspector) serDe.getObjectInspector();

    // Deserialize
    final Object row = serDe.deserialize(t);
    assertEquals("deserialization gives the wrong object class", row.getClass(), ArrayWritable.class);
    assertEquals("size correct after deserialization", serDe.getSerDeStats().getRawDataSize(), t.get().length);
    assertEquals("deserialization gives the wrong object", t, row);

    // Serialize
    final ParquetHiveRecord serializedArr = (ParquetHiveRecord) serDe.serialize(row, oi);
    assertEquals("size correct after serialization", serDe.getSerDeStats().getRawDataSize(), ((ArrayWritable)serializedArr.getObject()).get().length);
    assertTrue("serialized object should be equal to starting object", arrayWritableEquals(t, (ArrayWritable)serializedArr.getObject()));
  }

  private Properties createProperties() {
    final Properties tbl = new Properties();

    // Set the configuration parameters
    tbl.setProperty("columns", "abyte,ashort,aint,along,adouble,astring,abinary,amap,alist");
    tbl.setProperty("columns.types",
      "tinyint:smallint:int:bigint:double:string:binary:map<string,int>:array<string>");
    tbl.setProperty(org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_NULL_FORMAT, "NULL");
    return tbl;
  }

  public static boolean arrayWritableEquals(final ArrayWritable a1, final ArrayWritable a2) {
    final Writable[] a1Arr = a1.get();
    final Writable[] a2Arr = a2.get();

    if (a1Arr.length != a2Arr.length) {
      return false;
    }

    for (int i = 0; i < a1Arr.length; ++i) {
      if (a1Arr[i] instanceof ArrayWritable) {
        if (!(a2Arr[i] instanceof ArrayWritable)) {
          return false;
        }
        if (!arrayWritableEquals((ArrayWritable) a1Arr[i], (ArrayWritable) a2Arr[i])) {
          return false;
        }
      } else {
        if (!a1Arr[i].equals(a2Arr[i])) {
          return false;
        }
      }

    }
    return true;
  }

}
