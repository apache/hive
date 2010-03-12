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


import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.io.RowResult;
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

import junit.framework.TestCase;

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
    HbaseMapWritable<byte[], Cell> cells =
      new HbaseMapWritable<byte[], Cell>();
    cells.put(colabyte,    new Cell("123".getBytes(), 0));
    cells.put(colbshort,   new Cell("456".getBytes(), 0));
    cells.put(colcint,     new Cell("789".getBytes(), 0));
    cells.put(colalong,    new Cell("1000".getBytes(), 0));
    cells.put(colbdouble,  new Cell("5.3".getBytes(), 0));
    cells.put(colcstring,  new Cell("hive and hadoop".getBytes(), 0));
    RowResult rr = new RowResult("test-row1".getBytes(), cells);
    BatchUpdate bu = new BatchUpdate("test-row1".getBytes());
    bu.put(colabyte,    "123".getBytes());
    bu.put(colbshort,   "456".getBytes());
    bu.put(colcint,     "789".getBytes());
    bu.put(colalong,    "1000".getBytes());
    bu.put(colbdouble,  "5.3".getBytes());
    bu.put(colcstring,  "hive and hadoop".getBytes());
      
    Object[] expectedFieldsData = {
      new Text("test-row1"),
      new ByteWritable((byte)123),
      new ShortWritable((short)456),
      new IntWritable(789),
      new LongWritable(1000),
      new DoubleWritable(5.3),
      new Text("hive and hadoop")
    };
     
    deserializeAndSerialize(serDe, rr, bu, expectedFieldsData);
  }

  private void deserializeAndSerialize(
    HBaseSerDe serDe, RowResult rr, BatchUpdate bu,
      Object[] expectedFieldsData) throws SerDeException {

    // Get the row structure
    StructObjectInspector oi = (StructObjectInspector)
      serDe.getObjectInspector();
    List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
    assertEquals(7, fieldRefs.size());
    
    // Deserialize
    Object row = serDe.deserialize(rr);
    for (int i = 0; i < fieldRefs.size(); i++) {
      Object fieldData = oi.getStructFieldData(row, fieldRefs.get(i));
      if (fieldData != null) {
        fieldData = ((LazyPrimitive)fieldData).getWritableObject();
      }
      assertEquals("Field " + i, expectedFieldsData[i], fieldData);
    }
    // Serialize 
    assertEquals(BatchUpdate.class, serDe.getSerializedClass());
    BatchUpdate serializedBU = (BatchUpdate)serDe.serialize(row, oi);
    assertEquals("Serialized data", bu.toString(), serializedBU.toString());
  }

  private Properties createProperties() {
    Properties tbl = new Properties();
    
    // Set the configuration parameters
    tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");
    tbl.setProperty("columns",
        "key,abyte,ashort,aint,along,adouble,astring");
    tbl.setProperty("columns.types",
        "string,tinyint:smallint:int:bigint:double:string");
    tbl.setProperty(HBaseSerDe.HBASE_COL_MAPPING, 
        "cola:abyte,colb:ashort,colc:aint,cola:along,colb:adouble,colc:astring");
    return tbl;
  }
  
}
