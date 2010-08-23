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
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
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

    byte [] cfa = "cola".getBytes();
    byte [] cfb = "colb".getBytes();
    byte [] cfc = "colc".getBytes();

    byte [] qualByte = "byte".getBytes();
    byte [] qualShort = "short".getBytes();
    byte [] qualInt = "int".getBytes();
    byte [] qualLong = "long".getBytes();
    byte [] qualFloat = "float".getBytes();
    byte [] qualDouble = "double".getBytes();
    byte [] qualString = "string".getBytes();
    byte [] qualBool = "boolean".getBytes();

    byte [] rowKey = Bytes.toBytes("test-row1");

    // Data
    List<KeyValue> kvs = new ArrayList<KeyValue>();

    kvs.add(new KeyValue(rowKey, cfa, qualByte, Bytes.toBytes("123")));
    kvs.add(new KeyValue(rowKey, cfb, qualShort, Bytes.toBytes("456")));
    kvs.add(new KeyValue(rowKey, cfc, qualInt, Bytes.toBytes("789")));
    kvs.add(new KeyValue(rowKey, cfa, qualLong, Bytes.toBytes("1000")));
    kvs.add(new KeyValue(rowKey, cfb, qualFloat, Bytes.toBytes("-0.01")));
    kvs.add(new KeyValue(rowKey, cfc, qualDouble, Bytes.toBytes("5.3")));
    kvs.add(new KeyValue(rowKey, cfa, qualString, Bytes.toBytes("Hadoop, HBase, and Hive")));
    kvs.add(new KeyValue(rowKey, cfb, qualBool, Bytes.toBytes("true")));

    Result r = new Result(kvs);

    Put p = new Put(rowKey);

    p.add(cfa, qualByte, Bytes.toBytes("123"));
    p.add(cfb, qualShort, Bytes.toBytes("456"));
    p.add(cfc, qualInt, Bytes.toBytes("789"));
    p.add(cfa, qualLong, Bytes.toBytes("1000"));
    p.add(cfb, qualFloat, Bytes.toBytes("-0.01"));
    p.add(cfc, qualDouble, Bytes.toBytes("5.3"));
    p.add(cfa, qualString, Bytes.toBytes("Hadoop, HBase, and Hive"));
    p.add(cfb, qualBool, Bytes.toBytes("true"));

    Object[] expectedFieldsData = {
      new Text("test-row1"),
      new ByteWritable((byte)123),
      new ShortWritable((short)456),
      new IntWritable(789),
      new LongWritable(1000),
      new FloatWritable(-0.01F),
      new DoubleWritable(5.3),
      new Text("Hadoop, HBase, and Hive"),
      new BooleanWritable(true)
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
    assertEquals(9, fieldRefs.size());

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
    tbl.setProperty("columns", "key,abyte,ashort,aint,along,afloat,adouble,astring,abool");
    tbl.setProperty("columns.types",
        "string,tinyint:smallint:int:bigint:float:double:string:boolean");
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING,
        "cola:byte,colb:short,colc:int,cola:long,colb:float,colc:double,cola:string,colb:boolean");
    return tbl;
  }
}
