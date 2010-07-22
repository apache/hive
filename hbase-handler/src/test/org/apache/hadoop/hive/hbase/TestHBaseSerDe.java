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

package org.apache.hadoop.hive.hbase;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * Tests the HBaseSerDe class.
 */
public class TestHBaseSerDe extends TestCase {

  /**
   * Test the LazySimpleSerDe class.
   */
  public void testHBaseSerDe() throws SerDeException {
    // Create the SerDe
    HBaseSerDe serDe = new HBaseSerDe();
    Configuration conf = new Configuration();
    Properties tbl = createProperties();
    serDe.initialize(conf, tbl);

    byte[] colabyte   = "cola:abyte".getBytes();
    byte[] colbshort  = "colb:ashort".getBytes();
    byte[] colcint    = "colc:aint".getBytes();
    byte[] colalong   = "cola:along".getBytes();
    byte[] colbdouble = "colb:adouble".getBytes();
    byte[] colcstring = "colc:astring".getBytes();

    // Data
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    kvs.add(new KeyValue(Bytes.toBytes("test-row1"),
        colabyte, 0, Bytes.toBytes("123")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row1"),
        colbshort, 0, Bytes.toBytes("456")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row1"),
        colcint, 0, Bytes.toBytes("789")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row1"),
        colalong, 0, Bytes.toBytes("1000")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row1"),
        colbdouble, 0, Bytes.toBytes("5.3")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row1"),
        colcstring, 0, Bytes.toBytes("hive and hadoop")));
    Result r = new Result(kvs);

    Put p = new Put(Bytes.toBytes("test-row1"));

    p.add(colabyte, 0, Bytes.toBytes("123"));
    p.add(colbshort, 0, Bytes.toBytes("456"));
    p.add(colcint, 0, Bytes.toBytes("789"));
    p.add(colalong, 0, Bytes.toBytes("1000"));
    p.add(colbdouble, 0, Bytes.toBytes("5.3"));
    p.add(colcstring, 0, Bytes.toBytes("hive and hadoop"));

    Object[] expectedFieldsData = {
      new Text("test-row1"),
      new ByteWritable((byte)123),
      new ShortWritable((short)456),
      new IntWritable(789),
      new LongWritable(1000),
      new DoubleWritable(5.3),
      new Text("hive and hadoop")
    };

    deserializeAndSerialize(serDe, r, p, expectedFieldsData);
  }

  private void deserializeAndSerialize(
      HBaseSerDe serDe, Result r, Put p,
      Object[] expectedFieldsData) throws SerDeException {

    // Get the row structure
    StructObjectInspector oi = (StructObjectInspector)
      serDe.getObjectInspector();
    List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
    assertEquals(7, fieldRefs.size());

    // Deserialize
    Object row = serDe.deserialize(r);
    for (int i = 0; i < fieldRefs.size(); i++) {
      Object fieldData = oi.getStructFieldData(row, fieldRefs.get(i));
      if (fieldData != null) {
        fieldData = ((LazyPrimitive<?, ?>)fieldData).getWritableObject();
      }
      assertEquals("Field " + i, expectedFieldsData[i], fieldData);
    }
    // Serialize
    assertEquals(Put.class, serDe.getSerializedClass());
    Put serializedPut = (Put) serDe.serialize(row, oi);
    assertEquals("Serialized data", p.toString(), serializedPut.toString());
  }

  private Properties createProperties() {
    Properties tbl = new Properties();

    // Set the configuration parameters
    tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");
    tbl.setProperty("columns",
        "key,abyte,ashort,aint,along,adouble,astring");
    tbl.setProperty("columns.types",
        "string,tinyint:smallint:int:bigint:double:string");
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING,
        "cola:abyte,colb:ashort,colc:aint,cola:along,colb:adouble,colc:astring");
    return tbl;
  }
}
