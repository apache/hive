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
import java.util.Collections;
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
   * Test the default behavior of the Lazy family of objects and object inspectors.
   */
  public void testHBaseSerDeI() throws SerDeException {

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
    Collections.sort(kvs, KeyValue.COMPARATOR);

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

    // Create, initialize, and test the SerDe
    HBaseSerDe serDe = new HBaseSerDe();
    Configuration conf = new Configuration();
    Properties tbl = createPropertiesI_I();
    serDe.initialize(conf, tbl);

    deserializeAndSerialize(serDe, r, p, expectedFieldsData);

    serDe = new HBaseSerDe();
    conf = new Configuration();
    tbl = createPropertiesI_II();
    serDe.initialize(conf, tbl);

    deserializeAndSerialize(serDe, r, p, expectedFieldsData);

    serDe = new HBaseSerDe();
    conf = new Configuration();
    tbl = createPropertiesI_III();
    serDe.initialize(conf, tbl);

    deserializeAndSerialize(serDe, r, p, expectedFieldsData);

    serDe = new HBaseSerDe();
    conf = new Configuration();
    tbl = createPropertiesI_IV();
    serDe.initialize(conf, tbl);

    deserializeAndSerialize(serDe, r, p, expectedFieldsData);
  }

  public void testHBaseSerDeWithTimestamp() throws SerDeException {
    // Create the SerDe
    HBaseSerDe serDe = new HBaseSerDe();
    Configuration conf = new Configuration();
    Properties tbl = createPropertiesI_I();
    long putTimestamp = 1;
    tbl.setProperty(HBaseSerDe.HBASE_PUT_TIMESTAMP,
            Long.toString(putTimestamp));
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
    Collections.sort(kvs, KeyValue.COMPARATOR);

    Result r = new Result(kvs);

    Put p = new Put(rowKey,putTimestamp);

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

  // No specifications default to UTF8 String storage for backwards compatibility
  private Properties createPropertiesI_I() {
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

  // Default column storage specification inherits from table level default
  // (in this case a missing specification) of UTF String storage
  private Properties createPropertiesI_II() {
    Properties tbl = new Properties();

    // Set the configuration parameters
    tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");
    tbl.setProperty("columns", "key,abyte,ashort,aint,along,afloat,adouble,astring,abool");
    tbl.setProperty("columns.types",
        "string,tinyint:smallint:int:bigint:float:double:string:boolean");
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING,
        ":key#-,cola:byte#s,colb:short#-,colc:int#s,cola:long#s,colb:float#-,colc:double#-," +
        "cola:string#s,colb:boolean#s");
    return tbl;
  }

  // String storage type overrides table level default of binary storage
  private Properties createPropertiesI_III() {
    Properties tbl = new Properties();

    // Set the configuration parameters
    tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");
    tbl.setProperty("columns", "key,abyte,ashort,aint,along,afloat,adouble,astring,abool");
    tbl.setProperty("columns.types",
        "string,tinyint:smallint:int:bigint:float:double:string:boolean");
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING,
        ":key#s,cola:byte#s,colb:short#s,colc:int#s,cola:long#s,colb:float#s,colc:double#s," +
        "cola:string#s,colb:boolean#s");
    tbl.setProperty(HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE, "binary");
    return tbl;
  }

  // String type is never stored as anything other than an escaped string
  // A specification of binary storage should not affect ser/de.
  private Properties createPropertiesI_IV() {
    Properties tbl = new Properties();

    // Set the configuration parameters
    tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");
    tbl.setProperty("columns", "key,abyte,ashort,aint,along,afloat,adouble,astring,abool");
    tbl.setProperty("columns.types",
        "string,tinyint:smallint:int:bigint:float:double:string:boolean");
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING,
        ":key#-,cola:byte#s,colb:short#s,colc:int#s,cola:long#s,colb:float#s,colc:double#s," +
        "cola:string#b,colb:boolean#s");
    tbl.setProperty(HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE, "binary");
    return tbl;
  }

  public void testHBaseSerDeII() throws SerDeException {

    byte [] cfa = "cfa".getBytes();
    byte [] cfb = "cfb".getBytes();
    byte [] cfc = "cfc".getBytes();

    byte [] qualByte = "byte".getBytes();
    byte [] qualShort = "short".getBytes();
    byte [] qualInt = "int".getBytes();
    byte [] qualLong = "long".getBytes();
    byte [] qualFloat = "float".getBytes();
    byte [] qualDouble = "double".getBytes();
    byte [] qualString = "string".getBytes();
    byte [] qualBool = "boolean".getBytes();

    byte [] rowKey = Bytes.toBytes("test-row-2");

    // Data
    List<KeyValue> kvs = new ArrayList<KeyValue>();

    kvs.add(new KeyValue(rowKey, cfa, qualByte, new byte [] { Byte.MIN_VALUE }));
    kvs.add(new KeyValue(rowKey, cfb, qualShort, Bytes.toBytes(Short.MIN_VALUE)));
    kvs.add(new KeyValue(rowKey, cfc, qualInt, Bytes.toBytes(Integer.MIN_VALUE)));
    kvs.add(new KeyValue(rowKey, cfa, qualLong, Bytes.toBytes(Long.MIN_VALUE)));
    kvs.add(new KeyValue(rowKey, cfb, qualFloat, Bytes.toBytes(Float.MIN_VALUE)));
    kvs.add(new KeyValue(rowKey, cfc, qualDouble, Bytes.toBytes(Double.MAX_VALUE)));
    kvs.add(new KeyValue(rowKey, cfa, qualString, Bytes.toBytes(
      "Hadoop, HBase, and Hive Again!")));
    kvs.add(new KeyValue(rowKey, cfb, qualBool, Bytes.toBytes(false)));

    Collections.sort(kvs, KeyValue.COMPARATOR);
    Result r = new Result(kvs);

    Put p = new Put(rowKey);

    p.add(cfa, qualByte, new byte [] { Byte.MIN_VALUE });
    p.add(cfb, qualShort, Bytes.toBytes(Short.MIN_VALUE));
    p.add(cfc, qualInt, Bytes.toBytes(Integer.MIN_VALUE));
    p.add(cfa, qualLong, Bytes.toBytes(Long.MIN_VALUE));
    p.add(cfb, qualFloat, Bytes.toBytes(Float.MIN_VALUE));
    p.add(cfc, qualDouble, Bytes.toBytes(Double.MAX_VALUE));
    p.add(cfa, qualString, Bytes.toBytes("Hadoop, HBase, and Hive Again!"));
    p.add(cfb, qualBool, Bytes.toBytes(false));

    Object[] expectedFieldsData = {
      new Text("test-row-2"),
      new ByteWritable(Byte.MIN_VALUE),
      new ShortWritable(Short.MIN_VALUE),
      new IntWritable(Integer.MIN_VALUE),
      new LongWritable(Long.MIN_VALUE),
      new FloatWritable(Float.MIN_VALUE),
      new DoubleWritable(Double.MAX_VALUE),
      new Text("Hadoop, HBase, and Hive Again!"),
      new BooleanWritable(false)
    };

    // Create, initialize, and test the SerDe
    HBaseSerDe serDe = new HBaseSerDe();
    Configuration conf = new Configuration();
    Properties tbl = createPropertiesII_I();
    serDe.initialize(conf, tbl);

    deserializeAndSerialize(serDe, r, p, expectedFieldsData);

    serDe = new HBaseSerDe();
    conf = new Configuration();
    tbl = createPropertiesII_II();
    serDe.initialize(conf, tbl);

    deserializeAndSerialize(serDe, r, p, expectedFieldsData);

    serDe = new HBaseSerDe();
    conf = new Configuration();
    tbl = createPropertiesII_III();
    serDe.initialize(conf, tbl);

    deserializeAndSerialize(serDe, r, p, expectedFieldsData);
  }

  private Properties createPropertiesII_I() {
    Properties tbl = new Properties();

    // Set the configuration parameters
    tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");
    tbl.setProperty("columns", "key,abyte,ashort,aint,along,afloat,adouble,astring,abool");
    tbl.setProperty("columns.types",
        "string,tinyint:smallint:int:bigint:float:double:string:boolean");
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING,
        ":key#-,cfa:byte#b,cfb:short#b,cfc:int#-,cfa:long#b,cfb:float#-,cfc:double#b," +
        "cfa:string#b,cfb:boolean#-");
    tbl.setProperty(HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE, "binary");
    return tbl;
  }

  private Properties createPropertiesII_II() {
    Properties tbl = new Properties();

    // Set the configuration parameters
    tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");
    tbl.setProperty("columns", "key,abyte,ashort,aint,along,afloat,adouble,astring,abool");
    tbl.setProperty("columns.types",
        "string,tinyint:smallint:int:bigint:float:double:string:boolean");
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING,
        ":key#b,cfa:byte#b,cfb:short#b,cfc:int#b,cfa:long#b,cfb:float#b,cfc:double#b," +
        "cfa:string#b,cfb:boolean#b");
    tbl.setProperty(HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE, "string");
    return tbl;
  }

  private Properties createPropertiesII_III() {
    Properties tbl = new Properties();

    // Set the configuration parameters
    tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");
    tbl.setProperty("columns", "key,abyte,ashort,aint,along,afloat,adouble,astring,abool");
    tbl.setProperty("columns.types",
        "string,tinyint:smallint:int:bigint:float:double:string:boolean");
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING,
        ":key#-,cfa:byte#b,cfb:short#b,cfc:int#b,cfa:long#b,cfb:float#b,cfc:double#b," +
        "cfa:string#-,cfb:boolean#b");
    return tbl;
  }

  public void testHBaseSerDeWithHiveMapToHBaseColumnFamily() throws SerDeException {

    byte [] cfint = "cf-int".getBytes();
    byte [] cfbyte = "cf-byte".getBytes();
    byte [] cfshort = "cf-short".getBytes();
    byte [] cflong = "cf-long".getBytes();
    byte [] cffloat = "cf-float".getBytes();
    byte [] cfdouble = "cf-double".getBytes();
    byte [] cfbool = "cf-bool".getBytes();

    byte [][] columnFamilies =
      new byte [][] {cfint, cfbyte, cfshort, cflong, cffloat, cfdouble, cfbool};

    byte [][] rowKeys = new byte [][] {
        Integer.toString(1).getBytes(),
        Integer.toString(Integer.MIN_VALUE).getBytes(),
        Integer.toString(Integer.MAX_VALUE).getBytes()
    };

    byte [][][] columnQualifiersAndValues = new byte [][][] {
        {Bytes.toBytes(1), new byte [] {1}, Bytes.toBytes((short) 1),
         Bytes.toBytes((long) 1), Bytes.toBytes((float) 1.0F), Bytes.toBytes(1.0),
         Bytes.toBytes(true)},
        {Bytes.toBytes(Integer.MIN_VALUE), new byte [] {Byte.MIN_VALUE},
         Bytes.toBytes((short) Short.MIN_VALUE), Bytes.toBytes((long) Long.MIN_VALUE),
         Bytes.toBytes((float) Float.MIN_VALUE), Bytes.toBytes(Double.MIN_VALUE),
         Bytes.toBytes(false)},
        {Bytes.toBytes(Integer.MAX_VALUE), new byte [] {Byte.MAX_VALUE},
         Bytes.toBytes((short) Short.MAX_VALUE), Bytes.toBytes((long) Long.MAX_VALUE),
         Bytes.toBytes((float) Float.MAX_VALUE), Bytes.toBytes(Double.MAX_VALUE),
         Bytes.toBytes(true)}
    };

    List<KeyValue> kvs = new ArrayList<KeyValue>();
    Result [] r = new Result [] {null, null, null};
    Put [] p = new Put [] {null, null, null};

    for (int i = 0; i < r.length; i++) {
      kvs.clear();
      p[i] = new Put(rowKeys[i]);

      for (int j = 0; j < columnQualifiersAndValues[i].length; j++) {
        kvs.add(new KeyValue(rowKeys[i], columnFamilies[j], columnQualifiersAndValues[i][j],
            columnQualifiersAndValues[i][j]));
        p[i].add(columnFamilies[j], columnQualifiersAndValues[i][j],
            columnQualifiersAndValues[i][j]);
      }

      r[i] = new Result(kvs);
    }

    Object [][] expectedData = {
        {new Text(Integer.toString(1)), new IntWritable(1), new ByteWritable((byte) 1),
         new ShortWritable((short) 1), new LongWritable(1), new FloatWritable(1.0F),
         new DoubleWritable(1.0), new BooleanWritable(true)},
        {new Text(Integer.toString(Integer.MIN_VALUE)), new IntWritable(Integer.MIN_VALUE),
         new ByteWritable(Byte.MIN_VALUE), new ShortWritable(Short.MIN_VALUE),
         new LongWritable(Long.MIN_VALUE), new FloatWritable(Float.MIN_VALUE),
         new DoubleWritable(Double.MIN_VALUE), new BooleanWritable(false)},
        {new Text(Integer.toString(Integer.MAX_VALUE)), new IntWritable(Integer.MAX_VALUE),
         new ByteWritable(Byte.MAX_VALUE), new ShortWritable(Short.MAX_VALUE),
         new LongWritable(Long.MAX_VALUE), new FloatWritable(Float.MAX_VALUE),
         new DoubleWritable(Double.MAX_VALUE), new BooleanWritable(true)}};

    HBaseSerDe hbaseSerDe = new HBaseSerDe();
    Configuration conf = new Configuration();
    Properties tbl = createPropertiesForHiveMapHBaseColumnFamily();
    hbaseSerDe.initialize(conf, tbl);

    deserializeAndSerializeHiveMapHBaseColumnFamily(hbaseSerDe, r, p, expectedData, rowKeys,
        columnFamilies, columnQualifiersAndValues);

    hbaseSerDe = new HBaseSerDe();
    conf = new Configuration();
    tbl = createPropertiesForHiveMapHBaseColumnFamilyII();
    hbaseSerDe.initialize(conf, tbl);

    deserializeAndSerializeHiveMapHBaseColumnFamily(hbaseSerDe, r, p, expectedData, rowKeys,
        columnFamilies, columnQualifiersAndValues);
  }

  private void deserializeAndSerializeHiveMapHBaseColumnFamily(
      HBaseSerDe hbaseSerDe,
      Result [] r,
      Put [] p,
      Object [][] expectedData,
      byte [][] rowKeys,
      byte [][] columnFamilies,
      byte [][][] columnQualifiersAndValues) throws SerDeException {

    StructObjectInspector soi = (StructObjectInspector) hbaseSerDe.getObjectInspector();
    List<? extends StructField> fieldRefs = soi.getAllStructFieldRefs();
    assertEquals(8, fieldRefs.size());

    // Deserialize
    for (int i = 0; i < r.length; i++) {
      Object row = hbaseSerDe.deserialize(r[i]);
      Put serializedPut = (Put) hbaseSerDe.serialize(row, soi);
      byte [] rowKey = serializedPut.getRow();

      for (int k = 0; k < rowKey.length; k++) {
        assertEquals(rowKey[k], rowKeys[i][k]);
      }

      assertEquals(columnFamilies.length, serializedPut.numFamilies());

      for (int j = 0; j < fieldRefs.size(); j++) {
        Object fieldData = soi.getStructFieldData(row, fieldRefs.get(j));

        assertNotNull(fieldData);

        if (fieldData instanceof LazyPrimitive<?, ?>) {
          assertEquals(expectedData[i][j],
              ((LazyPrimitive<?, ?>) fieldData).getWritableObject());
        } else if (fieldData instanceof LazyHBaseCellMap) {
          LazyPrimitive<?, ?> lazyPrimitive = (LazyPrimitive<?, ?>)
              ((LazyHBaseCellMap) fieldData).getMapValueElement(expectedData[i][j]);
          assertEquals(expectedData[i][j], lazyPrimitive.getWritableObject());
        } else {
          fail("Error: field data not an instance of LazyPrimitive<?,?> or LazyMap");
        }
      }
    }
  }

  private Properties createPropertiesForHiveMapHBaseColumnFamily() {
    Properties tbl = new Properties();
    tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");
    tbl.setProperty(Constants.LIST_COLUMNS,
        "key,valint,valbyte,valshort,vallong,valfloat,valdouble,valbool");
    tbl.setProperty(Constants.LIST_COLUMN_TYPES,
        "string:map<int,int>:map<tinyint,tinyint>:map<smallint,smallint>:map<bigint,bigint>:"
        + "map<float,float>:map<double,double>:map<boolean,boolean>");
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING,
        ":key#-,cf-int:#b:b,cf-byte:#b:b,cf-short:#b:b,cf-long:#b:b,cf-float:#b:b,cf-double:#b:b," +
        "cf-bool:#b:b");
    return tbl;
  }

  private Properties createPropertiesForHiveMapHBaseColumnFamilyII() {
    Properties tbl = new Properties();
    tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");
    tbl.setProperty(Constants.LIST_COLUMNS,
        "key,valint,valbyte,valshort,vallong,valfloat,valdouble,valbool");
    tbl.setProperty(Constants.LIST_COLUMN_TYPES,
        "string:map<int,int>:map<tinyint,tinyint>:map<smallint,smallint>:map<bigint,bigint>:"
        + "map<float,float>:map<double,double>:map<boolean,boolean>");
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING,
        ":key#-,cf-int:#-:-,cf-byte:#-:-,cf-short:#-:-,cf-long:#-:-,cf-float:#-:-,cf-double:#-:-," +
        "cf-bool:#-:-");
    tbl.setProperty(HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE, "binary");
    return tbl;
  }

  public void testHBaseSerDeWithHiveMapToHBaseColumnFamilyII() throws SerDeException {

    byte [] cfbyte = "cf-byte".getBytes();
    byte [] cfshort = "cf-short".getBytes();
    byte [] cfint = "cf-int".getBytes();
    byte [] cflong = "cf-long".getBytes();
    byte [] cffloat = "cf-float".getBytes();
    byte [] cfdouble = "cf-double".getBytes();
    byte [] cfstring = "cf-string".getBytes();
    byte [] cfbool = "cf-bool".getBytes();

    byte [][] columnFamilies =
      new byte [][] {cfbyte, cfshort, cfint, cflong, cffloat, cfdouble, cfstring, cfbool};

    byte [] rowKey = Bytes.toBytes("row-key");

    byte [][] columnQualifiersAndValues = new byte [][] {
        Bytes.toBytes("123"), Bytes.toBytes("456"), Bytes.toBytes("789"), Bytes.toBytes("1000"),
        Bytes.toBytes("-0.01"), Bytes.toBytes("5.3"), Bytes.toBytes("Hive"),
        Bytes.toBytes("true")
    };

    Put p = new Put(rowKey);
    List<KeyValue> kvs = new ArrayList<KeyValue>();

    for (int j = 0; j < columnQualifiersAndValues.length; j++) {
      kvs.add(new KeyValue(rowKey,
          columnFamilies[j], columnQualifiersAndValues[j], columnQualifiersAndValues[j]));
      p.add(columnFamilies[j], columnQualifiersAndValues[j], columnQualifiersAndValues[j]);
    }

    Result r = new Result(kvs);

    Object [] expectedData = {
        new Text("row-key"), new ByteWritable((byte) 123), new ShortWritable((short) 456),
        new IntWritable(789), new LongWritable(1000), new FloatWritable(-0.01F),
        new DoubleWritable(5.3), new Text("Hive"), new BooleanWritable(true)
    };

    HBaseSerDe hbaseSerDe = new HBaseSerDe();
    Configuration conf = new Configuration();
    Properties tbl = createPropertiesForHiveMapHBaseColumnFamilyII_I();
    hbaseSerDe.initialize(conf, tbl);

    deserializeAndSerializeHiveMapHBaseColumnFamilyII(hbaseSerDe, r, p, expectedData,
        columnFamilies, columnQualifiersAndValues);

    hbaseSerDe = new HBaseSerDe();
    conf = new Configuration();
    tbl = createPropertiesForHiveMapHBaseColumnFamilyII_II();
    hbaseSerDe.initialize(conf, tbl);

    deserializeAndSerializeHiveMapHBaseColumnFamilyII(hbaseSerDe, r, p, expectedData,
        columnFamilies, columnQualifiersAndValues);
  }

  private Properties createPropertiesForHiveMapHBaseColumnFamilyII_I() {
    Properties tbl = new Properties();
    tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");
    tbl.setProperty(Constants.LIST_COLUMNS,
        "key,valbyte,valshort,valint,vallong,valfloat,valdouble,valstring,valbool");
    tbl.setProperty(Constants.LIST_COLUMN_TYPES,
        "string:map<tinyint,tinyint>:map<smallint,smallint>:map<int,int>:map<bigint,bigint>:"
        + "map<float,float>:map<double,double>:map<string,string>:map<boolean,boolean>");
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING,
        ":key#s,cf-byte:#-:s,cf-short:#s:-,cf-int:#s:s,cf-long:#-:-,cf-float:#s:-,cf-double:#-:s," +
        "cf-string:#s:s,cf-bool:#-:-");
    return tbl;
  }

  private Properties createPropertiesForHiveMapHBaseColumnFamilyII_II() {
    Properties tbl = new Properties();
    tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");
    tbl.setProperty(Constants.LIST_COLUMNS,
        "key,valbyte,valshort,valint,vallong,valfloat,valdouble,valstring,valbool");
    tbl.setProperty(Constants.LIST_COLUMN_TYPES,
        "string:map<tinyint,tinyint>:map<smallint,smallint>:map<int,int>:map<bigint,bigint>:"
        + "map<float,float>:map<double,double>:map<string,string>:map<boolean,boolean>");
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING,
        ":key#s,cf-byte:#s:s,cf-short:#s:s,cf-int:#s:s,cf-long:#s:s,cf-float:#s:s,cf-double:#s:s," +
        "cf-string:#s:s,cf-bool:#s:s");
    tbl.setProperty(HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE, "binary");
    return tbl;
  }

  private void deserializeAndSerializeHiveMapHBaseColumnFamilyII(
      HBaseSerDe hbaseSerDe,
      Result r,
      Put p,
      Object [] expectedData,
      byte [][] columnFamilies,
      byte [][] columnQualifiersAndValues) throws SerDeException {

    StructObjectInspector soi = (StructObjectInspector) hbaseSerDe.getObjectInspector();
    List<? extends StructField> fieldRefs = soi.getAllStructFieldRefs();
    assertEquals(9, fieldRefs.size());

    // Deserialize
    Object row = hbaseSerDe.deserialize(r);

    for (int j = 0; j < fieldRefs.size(); j++) {
      Object fieldData = soi.getStructFieldData(row, fieldRefs.get(j));
      assertNotNull(fieldData);

      if (fieldData instanceof LazyPrimitive<?, ?>) {
        assertEquals(expectedData[j], ((LazyPrimitive<?, ?>) fieldData).getWritableObject());
      } else if (fieldData instanceof LazyHBaseCellMap) {
        LazyPrimitive<?, ?> lazyPrimitive = (LazyPrimitive<?, ?>)
          ((LazyHBaseCellMap) fieldData).getMapValueElement(expectedData[j]);
        assertEquals(expectedData[j], lazyPrimitive.getWritableObject());
      } else {
        fail("Error: field data not an instance of LazyPrimitive<?, ?> or LazyHBaseCellMap");
      }
    }

    // Serialize
    Put serializedPut = (Put) hbaseSerDe.serialize(row, soi);
    assertEquals("Serialized data: ", p.toString(), serializedPut.toString());
  }
}
