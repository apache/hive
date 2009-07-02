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
package org.apache.hadoop.hive.serde2.lazy;


import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

public class TestLazySimpleSerDe extends TestCase {

  /**
   * Test the LazySimpleSerDe class.
   */
  public void testLazySimpleSerDe() throws Throwable {
    try {
      // Create the SerDe
      LazySimpleSerDe serDe = new LazySimpleSerDe();
      Configuration conf = new Configuration();
      Properties tbl = createProperties();
      serDe.initialize(conf, tbl);

      // Data
      Text t = new Text("123\t456\t789\t1000\t5.3\thive and hadoop\t1.\tNULL");
      String s = "123\t456\t789\t1000\t5.3\thive and hadoop\tNULL\tNULL"; 
      Object[] expectedFieldsData = { new ByteWritable((byte)123),
          new ShortWritable((short)456), new IntWritable(789),
          new LongWritable(1000), new DoubleWritable(5.3), new Text("hive and hadoop"), null,
          null
      };
      
      // Test
      deserializeAndSerialize(serDe, t, s, expectedFieldsData);
     
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  private void deserializeAndSerialize(LazySimpleSerDe serDe, Text t, String s,
      Object[] expectedFieldsData) throws SerDeException {
    // Get the row structure
    StructObjectInspector oi = (StructObjectInspector)serDe.getObjectInspector();
    List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
    assertEquals(8, fieldRefs.size());
    
    // Deserialize
    Object row = serDe.deserialize(t);
    for (int i = 0; i < fieldRefs.size(); i++) {
      Object fieldData = oi.getStructFieldData(row, fieldRefs.get(i));
      if (fieldData != null) {
        fieldData = ((LazyPrimitive)fieldData).getWritableObject();
      }
      assertEquals("Field " + i, expectedFieldsData[i], fieldData);
    }
    // Serialize 
    assertEquals(Text.class, serDe.getSerializedClass());
    Text serializedText = (Text)serDe.serialize(row, oi);
    assertEquals("Serialized data", s, serializedText.toString());
  }

  private Properties createProperties() {
    Properties tbl = new Properties();
    
    // Set the configuration parameters
    tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");
    tbl.setProperty("columns",
        "abyte,ashort,aint,along,adouble,astring,anullint,anullstring");
    tbl.setProperty("columns.types",
        "tinyint:smallint:int:bigint:double:string:int:string");
    tbl.setProperty(Constants.SERIALIZATION_NULL_FORMAT, "NULL");
    return tbl;
  }
    
  /**
   * Test the LazySimpleSerDe class with LastColumnTakesRest option.
   */
  public void testLazySimpleSerDeLastColumnTakesRest() throws Throwable {
    try {
      // Create the SerDe
      LazySimpleSerDe serDe = new LazySimpleSerDe();
      Configuration conf = new Configuration();
      Properties tbl = createProperties();
      tbl.setProperty(Constants.SERIALIZATION_LAST_COLUMN_TAKES_REST, "true");
      serDe.initialize(conf, tbl);
      
      // Data
      Text t = new Text("123\t456\t789\t1000\t5.3\thive and hadoop\t1.\ta\tb\t");
      String s = "123\t456\t789\t1000\t5.3\thive and hadoop\tNULL\ta\tb\t"; 
      Object[] expectedFieldsData = { new ByteWritable((byte)123),
          new ShortWritable((short)456), new IntWritable(789),
          new LongWritable(1000), new DoubleWritable(5.3), new Text("hive and hadoop"), null,
          new Text("a\tb\t")
      };
      
      // Test
      deserializeAndSerialize(serDe, t, s, expectedFieldsData);

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  
  /**
   * Test the LazySimpleSerDe class with extra columns.
   */
  public void testLazySimpleSerDeExtraColumns() throws Throwable {
    try {
      // Create the SerDe
      LazySimpleSerDe serDe = new LazySimpleSerDe();
      Configuration conf = new Configuration();
      Properties tbl = createProperties();
      serDe.initialize(conf, tbl);
      
      // Data
      Text t = new Text("123\t456\t789\t1000\t5.3\thive and hadoop\t1.\ta\tb\t");
      String s = "123\t456\t789\t1000\t5.3\thive and hadoop\tNULL\ta"; 
      Object[] expectedFieldsData = { new ByteWritable((byte)123),
          new ShortWritable((short)456), new IntWritable(789),
          new LongWritable(1000), new DoubleWritable(5.3), new Text("hive and hadoop"), null,
          new Text("a")
      };
      
      // Test
      deserializeAndSerialize(serDe, t, s, expectedFieldsData);

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  
  /**
   * Test the LazySimpleSerDe class with missing columns.
   */
  public void testLazySimpleSerDeMissingColumns() throws Throwable {
    try {
      // Create the SerDe
      LazySimpleSerDe serDe = new LazySimpleSerDe();
      Configuration conf = new Configuration();
      Properties tbl = createProperties();
      serDe.initialize(conf, tbl);
      
      // Data
      Text t = new Text("123\t456\t789\t1000\t5.3\t");
      String s = "123\t456\t789\t1000\t5.3\t\tNULL\tNULL"; 
      Object[] expectedFieldsData = { new ByteWritable((byte)123),
          new ShortWritable((short)456), new IntWritable(789),
          new LongWritable(1000), new DoubleWritable(5.3), new Text(""), null,
          null
      };
      
      // Test
      deserializeAndSerialize(serDe, t, s, expectedFieldsData);

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }
  
}
