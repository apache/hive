/*
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Assert;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.hbase.avro.Address;
import org.apache.hadoop.hive.hbase.avro.ContactInfo;
import org.apache.hadoop.hive.hbase.avro.Employee;
import org.apache.hadoop.hive.hbase.avro.Gender;
import org.apache.hadoop.hive.hbase.avro.HomePhone;
import org.apache.hadoop.hive.hbase.avro.OfficePhone;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.AvroTableProperties;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNull;
import org.junit.Test;

/**
 * Tests the HBaseSerDe class.
 */
public class TestHBaseSerDe {

  static final byte[] TEST_BYTE_ARRAY = Bytes.toBytes("test");

  private static final String RECORD_SCHEMA = "{\n" +
      "  \"namespace\": \"testing.test.mctesty\",\n" +
      "  \"name\": \"oneRecord\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "    {\n" +
      "      \"name\":\"aRecord\",\n" +
      "      \"type\":{\"type\":\"record\",\n" +
      "              \"name\":\"recordWithinARecord\",\n" +
      "              \"fields\": [\n" +
      "                 {\n" +
      "                  \"name\":\"int1\",\n" +
      "                  \"type\":\"int\"\n" +
      "                },\n" +
      "                {\n" +
      "                  \"name\":\"boolean1\",\n" +
      "                  \"type\":\"boolean\"\n" +
      "                },\n" +
      "                {\n" +
      "                  \"name\":\"long1\",\n" +
      "                  \"type\":\"long\"\n" +
      "                }\n" +
      "      ]}\n" +
      "    }\n" +
      "  ]\n" +
      "}";

  private static final String RECORD_SCHEMA_EVOLVED = "{\n" +
      "  \"namespace\": \"testing.test.mctesty\",\n" +
      "  \"name\": \"oneRecord\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "    {\n" +
      "      \"name\":\"aRecord\",\n" +
      "      \"type\":{\"type\":\"record\",\n" +
      "              \"name\":\"recordWithinARecord\",\n" +
      "              \"fields\": [\n" +
      "                 {\n" +
      "                  \"name\":\"int1\",\n" +
      "                  \"type\":\"int\"\n" +
      "                },\n" +
      "                {\n" +
      "                  \"name\":\"string1\",\n" +
      "                  \"type\":\"string\", \"default\": \"test\"\n" +
      "                },\n" +
      "                {\n" +
      "                  \"name\":\"boolean1\",\n" +
      "                  \"type\":\"boolean\"\n" +
      "                },\n" +
      "                {\n" +
      "                  \"name\":\"long1\",\n" +
      "                  \"type\":\"long\"\n" +
      "                }\n" +
      "      ]}\n" +
      "    }\n" +
      "  ]\n" +
      "}";

  private static final String EXPECTED_DESERIALIZED_AVRO_STRING =
      "{\"key\":\"test-row1\",\"cola_avro\":{\"arecord\":{\"int1\":42,\"boolean1\":true,"
          + "\"long1\":42432234234}}}";

  private static final String EXPECTED_DESERIALIZED_AVRO_STRING_2 =
 "{\"key\":\"test-row1\","
      + "\"cola_avro\":{\"employeename\":\"Avro Employee1\","
      + "\"employeeid\":11111,\"age\":25,\"gender\":\"FEMALE\","
      + "\"contactinfo\":{\"address\":[{\"address1\":\"Avro First Address1\",\"address2\":"
      + "\"Avro Second Address1\",\"city\":\"Avro City1\",\"zipcode\":123456,\"county\":"
      + "{0:{\"areacode\":999,\"number\":1234567890}},\"aliases\":null,\"metadata\":"
      + "{\"testkey\":\"testvalue\"}},{\"address1\":\"Avro First Address1\",\"address2\":"
      + "\"Avro Second Address1\",\"city\":\"Avro City1\",\"zipcode\":123456,\"county\":"
      + "{0:{\"areacode\":999,\"number\":1234567890}},\"aliases\":null,\"metadata\":"
      + "{\"testkey\":\"testvalue\"}}],\"homephone\":{\"areacode\":999,\"number\":1234567890},"
      + "\"officephone\":{\"areacode\":999,\"number\":1234455555}}}}";

  private static final String EXPECTED_DESERIALIZED_AVRO_STRING_3 =
      "{\"key\":\"test-row1\",\"cola_avro\":{\"arecord\":{\"int1\":42,\"string1\":\"test\","
          + "\"boolean1\":true,\"long1\":42432234234}}}";

  /**
   * Test the default behavior of the Lazy family of objects and object inspectors.
   */
  @Test
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
    List<Cell> kvs = new ArrayList<Cell>();

    kvs.add(new KeyValue(rowKey, cfa, qualByte, Bytes.toBytes("123")));
    kvs.add(new KeyValue(rowKey, cfb, qualShort, Bytes.toBytes("456")));
    kvs.add(new KeyValue(rowKey, cfc, qualInt, Bytes.toBytes("789")));
    kvs.add(new KeyValue(rowKey, cfa, qualLong, Bytes.toBytes("1000")));
    kvs.add(new KeyValue(rowKey, cfb, qualFloat, Bytes.toBytes("-0.01")));
    kvs.add(new KeyValue(rowKey, cfc, qualDouble, Bytes.toBytes("5.3")));
    kvs.add(new KeyValue(rowKey, cfa, qualString, Bytes.toBytes("Hadoop, HBase, and Hive")));
    kvs.add(new KeyValue(rowKey, cfb, qualBool, Bytes.toBytes("true")));
    Collections.sort(kvs, KeyValue.COMPARATOR);

    Result r = Result.create(kvs);

    Put p = new Put(rowKey);

    p.addColumn(cfa, qualByte, Bytes.toBytes("123"));
    p.addColumn(cfb, qualShort, Bytes.toBytes("456"));
    p.addColumn(cfc, qualInt, Bytes.toBytes("789"));
    p.addColumn(cfa, qualLong, Bytes.toBytes("1000"));
    p.addColumn(cfb, qualFloat, Bytes.toBytes("-0.01"));
    p.addColumn(cfc, qualDouble, Bytes.toBytes("5.3"));
    p.addColumn(cfa, qualString, Bytes.toBytes("Hadoop, HBase, and Hive"));
    p.addColumn(cfb, qualBool, Bytes.toBytes("true"));

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
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);

    deserializeAndSerialize(serDe, r, p, expectedFieldsData);

    serDe = new HBaseSerDe();
    conf = new Configuration();
    tbl = createPropertiesI_II();
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);

    deserializeAndSerialize(serDe, r, p, expectedFieldsData);

    serDe = new HBaseSerDe();
    conf = new Configuration();
    tbl = createPropertiesI_III();
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);

    deserializeAndSerialize(serDe, r, p, expectedFieldsData);

    serDe = new HBaseSerDe();
    conf = new Configuration();
    tbl = createPropertiesI_IV();
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);

    deserializeAndSerialize(serDe, r, p, expectedFieldsData);
  }

  @Test
  public void testHBaseSerDeWithTimestamp() throws SerDeException {
    // Create the SerDe
    HBaseSerDe serDe = new HBaseSerDe();
    Configuration conf = new Configuration();
    Properties tbl = createPropertiesI_I();
    long putTimestamp = 1;
    tbl.setProperty(HBaseSerDe.HBASE_PUT_TIMESTAMP,
            Long.toString(putTimestamp));
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);


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
    List<Cell> kvs = new ArrayList<Cell>();

    kvs.add(new KeyValue(rowKey, cfa, qualByte, Bytes.toBytes("123")));
    kvs.add(new KeyValue(rowKey, cfb, qualShort, Bytes.toBytes("456")));
    kvs.add(new KeyValue(rowKey, cfc, qualInt, Bytes.toBytes("789")));
    kvs.add(new KeyValue(rowKey, cfa, qualLong, Bytes.toBytes("1000")));
    kvs.add(new KeyValue(rowKey, cfb, qualFloat, Bytes.toBytes("-0.01")));
    kvs.add(new KeyValue(rowKey, cfc, qualDouble, Bytes.toBytes("5.3")));
    kvs.add(new KeyValue(rowKey, cfa, qualString, Bytes.toBytes("Hadoop, HBase, and Hive")));
    kvs.add(new KeyValue(rowKey, cfb, qualBool, Bytes.toBytes("true")));
    Collections.sort(kvs, KeyValue.COMPARATOR);

    Result r = Result.create(kvs);

    Put p = new Put(rowKey,putTimestamp);

    p.addColumn(cfa, qualByte, Bytes.toBytes("123"));
    p.addColumn(cfb, qualShort, Bytes.toBytes("456"));
    p.addColumn(cfc, qualInt, Bytes.toBytes("789"));
    p.addColumn(cfa, qualLong, Bytes.toBytes("1000"));
    p.addColumn(cfb, qualFloat, Bytes.toBytes("-0.01"));
    p.addColumn(cfc, qualDouble, Bytes.toBytes("5.3"));
    p.addColumn(cfa, qualString, Bytes.toBytes("Hadoop, HBase, and Hive"));
    p.addColumn(cfb, qualBool, Bytes.toBytes("true"));

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
    Object row = serDe.deserialize(new ResultWritable(r));
    for (int i = 0; i < fieldRefs.size(); i++) {
      Object fieldData = oi.getStructFieldData(row, fieldRefs.get(i));
      if (fieldData != null) {
        fieldData = ((LazyPrimitive<?, ?>)fieldData).getWritableObject();
      }
      assertEquals("Field " + i, expectedFieldsData[i], fieldData);
    }
    // Serialize
    assertEquals(PutWritable.class, serDe.getSerializedClass());
    PutWritable serializedPut = (PutWritable) serDe.serialize(row, oi);
    assertEquals("Serialized data", p.toString(),String.valueOf(serializedPut.getPut()));
  }

  // No specifications default to UTF8 String storage for backwards compatibility
  private Properties createPropertiesI_I() {
    Properties tbl = new Properties();

    // Set the configuration parameters
    tbl.setProperty(serdeConstants.SERIALIZATION_FORMAT, "9");
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
    tbl.setProperty(serdeConstants.SERIALIZATION_FORMAT, "9");
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
    tbl.setProperty(serdeConstants.SERIALIZATION_FORMAT, "9");
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
    tbl.setProperty(serdeConstants.SERIALIZATION_FORMAT, "9");
    tbl.setProperty("columns", "key,abyte,ashort,aint,along,afloat,adouble,astring,abool");
    tbl.setProperty("columns.types",
        "string,tinyint:smallint:int:bigint:float:double:string:boolean");
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING,
        ":key#-,cola:byte#s,colb:short#s,colc:int#s,cola:long#s,colb:float#s,colc:double#s," +
        "cola:string#b,colb:boolean#s");
    tbl.setProperty(HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE, "binary");
    return tbl;
  }

  @Test
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
    List<Cell> kvs = new ArrayList<Cell>();

    kvs.add(new KeyValue(rowKey, cfa, qualByte, new byte [] { Byte.MIN_VALUE }));
    kvs.add(new KeyValue(rowKey, cfb, qualShort, Bytes.toBytes(Short.MIN_VALUE)));
    kvs.add(new KeyValue(rowKey, cfc, qualInt, Bytes.toBytes(Integer.MIN_VALUE)));
    kvs.add(new KeyValue(rowKey, cfa, qualLong, Bytes.toBytes(Long.MIN_VALUE)));
    kvs.add(new KeyValue(rowKey, cfb, qualFloat, Bytes.toBytes(Float.MIN_VALUE)));
    kvs.add(new KeyValue(rowKey, cfc, qualDouble, Bytes.toBytes(Double.MAX_VALUE)));
    kvs.add(new KeyValue(rowKey, cfa, qualString, Bytes.toBytes(
      "Hadoop, HBase, and Hive Again!")));
    kvs.add(new KeyValue(rowKey, cfb, qualBool, Bytes.toBytes(false)));

//    When using only HBase2, then we could change to this
//    Collections.sort(kvs, CellComparator.COMPARATOR);
    Collections.sort(kvs, KeyValue.COMPARATOR);
    Result r = Result.create(kvs);

    Put p = new Put(rowKey);

    p.addColumn(cfa, qualByte, new byte [] { Byte.MIN_VALUE });
    p.addColumn(cfb, qualShort, Bytes.toBytes(Short.MIN_VALUE));
    p.addColumn(cfc, qualInt, Bytes.toBytes(Integer.MIN_VALUE));
    p.addColumn(cfa, qualLong, Bytes.toBytes(Long.MIN_VALUE));
    p.addColumn(cfb, qualFloat, Bytes.toBytes(Float.MIN_VALUE));
    p.addColumn(cfc, qualDouble, Bytes.toBytes(Double.MAX_VALUE));
    p.addColumn(cfa, qualString, Bytes.toBytes("Hadoop, HBase, and Hive Again!"));
    p.addColumn(cfb, qualBool, Bytes.toBytes(false));

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
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);

    deserializeAndSerialize(serDe, r, p, expectedFieldsData);

    serDe = new HBaseSerDe();
    conf = new Configuration();
    tbl = createPropertiesII_II();
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);

    deserializeAndSerialize(serDe, r, p, expectedFieldsData);

    serDe = new HBaseSerDe();
    conf = new Configuration();
    tbl = createPropertiesII_III();
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);

    deserializeAndSerialize(serDe, r, p, expectedFieldsData);
  }

  private Properties createPropertiesII_I() {
    Properties tbl = new Properties();

    // Set the configuration parameters
    tbl.setProperty(serdeConstants.SERIALIZATION_FORMAT, "9");
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
    tbl.setProperty(serdeConstants.SERIALIZATION_FORMAT, "9");
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
    tbl.setProperty(serdeConstants.SERIALIZATION_FORMAT, "9");
    tbl.setProperty("columns", "key,abyte,ashort,aint,along,afloat,adouble,astring,abool");
    tbl.setProperty("columns.types",
        "string,tinyint:smallint:int:bigint:float:double:string:boolean");
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING,
        ":key#-,cfa:byte#b,cfb:short#b,cfc:int#b,cfa:long#b,cfb:float#b,cfc:double#b," +
        "cfa:string#-,cfb:boolean#b");
    return tbl;
  }

  @Test
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
         Bytes.toBytes((long) 1), Bytes.toBytes(1.0F), Bytes.toBytes(1.0),
         Bytes.toBytes(true)},
        {Bytes.toBytes(Integer.MIN_VALUE), new byte [] {Byte.MIN_VALUE},
         Bytes.toBytes(Short.MIN_VALUE), Bytes.toBytes(Long.MIN_VALUE),
         Bytes.toBytes(Float.MIN_VALUE), Bytes.toBytes(Double.MIN_VALUE),
         Bytes.toBytes(false)},
        {Bytes.toBytes(Integer.MAX_VALUE), new byte [] {Byte.MAX_VALUE},
         Bytes.toBytes(Short.MAX_VALUE), Bytes.toBytes(Long.MAX_VALUE),
         Bytes.toBytes(Float.MAX_VALUE), Bytes.toBytes(Double.MAX_VALUE),
         Bytes.toBytes(true)}
    };

    List<Cell> kvs = new ArrayList<Cell>();
    Result [] r = new Result [] {null, null, null};
    Put [] p = new Put [] {null, null, null};

    for (int i = 0; i < r.length; i++) {
      kvs.clear();
      p[i] = new Put(rowKeys[i]);

      for (int j = 0; j < columnQualifiersAndValues[i].length; j++) {
        kvs.add(new KeyValue(rowKeys[i], columnFamilies[j], columnQualifiersAndValues[i][j],
            columnQualifiersAndValues[i][j]));
        p[i].addColumn(columnFamilies[j], columnQualifiersAndValues[i][j],
            columnQualifiersAndValues[i][j]);
      }

      r[i] = Result.create(kvs);
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
    SerDeUtils.initializeSerDe(hbaseSerDe, conf, tbl, null);

    deserializeAndSerializeHiveMapHBaseColumnFamily(hbaseSerDe, r, p, expectedData, rowKeys,
        columnFamilies, columnQualifiersAndValues);

    hbaseSerDe = new HBaseSerDe();
    conf = new Configuration();
    tbl = createPropertiesForHiveMapHBaseColumnFamilyII();
    SerDeUtils.initializeSerDe(hbaseSerDe, conf, tbl, null);

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
      Object row = hbaseSerDe.deserialize(new ResultWritable(r[i]));
      Put serializedPut = ((PutWritable) hbaseSerDe.serialize(row, soi)).getPut();
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
    tbl.setProperty(serdeConstants.SERIALIZATION_FORMAT, "9");
    tbl.setProperty(serdeConstants.LIST_COLUMNS,
        "key,valint,valbyte,valshort,vallong,valfloat,valdouble,valbool");
    tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES,
        "string:map<int,int>:map<tinyint,tinyint>:map<smallint,smallint>:map<bigint,bigint>:"
            + "map<float,float>:map<double,double>:map<boolean,boolean>");
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING,
        ":key#-,cf-int:#b:b,cf-byte:#b:b,cf-short:#b:b,cf-long:#b:b,cf-float:#b:b,cf-double:#b:b," +
        "cf-bool:#b:b");
    return tbl;
  }

  private Properties createPropertiesForHiveMapHBaseColumnFamilyII() {
    Properties tbl = new Properties();
    tbl.setProperty(serdeConstants.SERIALIZATION_FORMAT, "9");
    tbl.setProperty(serdeConstants.LIST_COLUMNS,
        "key,valint,valbyte,valshort,vallong,valfloat,valdouble,valbool");
    tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES,
        "string:map<int,int>:map<tinyint,tinyint>:map<smallint,smallint>:map<bigint,bigint>:"
            + "map<float,float>:map<double,double>:map<boolean,boolean>");
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING,
        ":key#-,cf-int:#-:-,cf-byte:#-:-,cf-short:#-:-,cf-long:#-:-,cf-float:#-:-,cf-double:#-:-," +
        "cf-bool:#-:-");
    tbl.setProperty(HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE, "binary");
    return tbl;
  }

  @Test
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
    List<Cell> kvs = new ArrayList<Cell>();

    for (int j = 0; j < columnQualifiersAndValues.length; j++) {
      kvs.add(new KeyValue(rowKey,
          columnFamilies[j], columnQualifiersAndValues[j], columnQualifiersAndValues[j]));
      p.addColumn(columnFamilies[j], columnQualifiersAndValues[j], columnQualifiersAndValues[j]);
    }

    Result r = Result.create(kvs);

    Object [] expectedData = {
        new Text("row-key"), new ByteWritable((byte) 123), new ShortWritable((short) 456),
        new IntWritable(789), new LongWritable(1000), new FloatWritable(-0.01F),
        new DoubleWritable(5.3), new Text("Hive"), new BooleanWritable(true)
    };

    HBaseSerDe hbaseSerDe = new HBaseSerDe();
    Configuration conf = new Configuration();
    Properties tbl = createPropertiesForHiveMapHBaseColumnFamilyII_I();
    SerDeUtils.initializeSerDe(hbaseSerDe, conf, tbl, null);

    deserializeAndSerializeHiveMapHBaseColumnFamilyII(hbaseSerDe, r, p, expectedData,
        columnFamilies, columnQualifiersAndValues);

    hbaseSerDe = new HBaseSerDe();
    conf = new Configuration();
    tbl = createPropertiesForHiveMapHBaseColumnFamilyII_II();
    SerDeUtils.initializeSerDe(hbaseSerDe, conf, tbl, null);

    deserializeAndSerializeHiveMapHBaseColumnFamilyII(hbaseSerDe, r, p, expectedData,
        columnFamilies, columnQualifiersAndValues);
  }

  private Properties createPropertiesForHiveMapHBaseColumnFamilyII_I() {
    Properties tbl = new Properties();
    tbl.setProperty(serdeConstants.SERIALIZATION_FORMAT, "9");
    tbl.setProperty(serdeConstants.LIST_COLUMNS,
        "key,valbyte,valshort,valint,vallong,valfloat,valdouble,valstring,valbool");
    tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES,
        "string:map<tinyint,tinyint>:map<smallint,smallint>:map<int,int>:map<bigint,bigint>:"
            + "map<float,float>:map<double,double>:map<string,string>:map<boolean,boolean>");
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING,
        ":key#s,cf-byte:#-:s,cf-short:#s:-,cf-int:#s:s,cf-long:#-:-,cf-float:#s:-,cf-double:#-:s," +
        "cf-string:#s:s,cf-bool:#-:-");
    return tbl;
  }

  private Properties createPropertiesForHiveMapHBaseColumnFamilyII_II() {
    Properties tbl = new Properties();
    tbl.setProperty(serdeConstants.SERIALIZATION_FORMAT, "9");
    tbl.setProperty(serdeConstants.LIST_COLUMNS,
        "key,valbyte,valshort,valint,vallong,valfloat,valdouble,valstring,valbool");
    tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES,
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
    Object row = hbaseSerDe.deserialize(new ResultWritable(r));

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
    Put serializedPut = ((PutWritable) hbaseSerDe.serialize(row, soi)).getPut();
    assertEquals("Serialized data: ", p.toString(), serializedPut.toString());
  }

  @Test
  public void testHBaseSerDeWithColumnPrefixes()
      throws Exception {
    byte[] cfa = "cola".getBytes();

    byte[] qualA = "prefixA_col1".getBytes();
    byte[] qualB = "prefixB_col2".getBytes();
    byte[] qualC = "prefixB_col3".getBytes();
    byte[] qualD = "unwanted_col".getBytes();

    List<Object> qualifiers = new ArrayList<Object>();
    qualifiers.add(new Text("prefixA_col1"));
    qualifiers.add(new Text("prefixB_col2"));
    qualifiers.add(new Text("prefixB_col3"));
    qualifiers.add(new Text("unwanted_col"));

    List<Object> expectedQualifiers = new ArrayList<Object>();
    expectedQualifiers.add(new Text("prefixA_col1"));
    expectedQualifiers.add(new Text("prefixB_col2"));
    expectedQualifiers.add(new Text("prefixB_col3"));

    byte[] rowKey = Bytes.toBytes("test-row1");

    // Data
    List<Cell> kvs = new ArrayList<Cell>();

    byte[] dataA = "This is first test data".getBytes();
    byte[] dataB = "This is second test data".getBytes();
    byte[] dataC = "This is third test data".getBytes();
    byte[] dataD = "Unwanted data".getBytes();

    kvs.add(new KeyValue(rowKey, cfa, qualA, dataA));
    kvs.add(new KeyValue(rowKey, cfa, qualB, dataB));
    kvs.add(new KeyValue(rowKey, cfa, qualC, dataC));
    kvs.add(new KeyValue(rowKey, cfa, qualD, dataD));

    Result r = Result.create(kvs);

    Put p = new Put(rowKey);

    p.add(new KeyValue(rowKey, cfa, qualA, dataA));
    p.add(new KeyValue(rowKey, cfa, qualB, dataB));
    p.add(new KeyValue(rowKey, cfa, qualC, dataC));

    Object[] expectedFieldsData = {
        new Text("test-row1"),
        new String("This is first test data"),
        new String("This is second test data"),
        new String("This is third test data")};

    int[] expectedMapSize = new int[] {1, 2};

    // Create, initialize, and test the SerDe
    HBaseSerDe serDe = new HBaseSerDe();
    Configuration conf = new Configuration();
    Properties tbl = createPropertiesForColumnPrefixes();
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);

    Object notPresentKey = new Text("unwanted_col");

    deserializeAndSerializeHivePrefixColumnFamily(serDe, r, p, expectedFieldsData, expectedMapSize,
        expectedQualifiers,
        notPresentKey);
  }

  private Properties createPropertiesForColumnPrefixes() {
    Properties tbl = new Properties();
    tbl.setProperty(serdeConstants.LIST_COLUMNS,
        "key,astring,along");
    tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES,
        "string:map<string,string>:map<string,string>");
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING,
        ":key,cola:prefixA_.*,cola:prefixB_.*");

    return tbl;
  }

  private void deserializeAndSerializeHivePrefixColumnFamily(HBaseSerDe serDe, Result r, Put p,
      Object[] expectedFieldsData, int[] expectedMapSize, List<Object> expectedQualifiers,
      Object notPresentKey)
      throws SerDeException, IOException {
    StructObjectInspector soi = (StructObjectInspector) serDe.getObjectInspector();

    List<? extends StructField> fieldRefs = soi.getAllStructFieldRefs();

    Object row = serDe.deserialize(new ResultWritable(r));

    int j = 0;

    for (int i = 0; i < fieldRefs.size(); i++) {
      Object fieldData = soi.getStructFieldData(row, fieldRefs.get(i));
      assertNotNull(fieldData);

      if (fieldData instanceof LazyPrimitive<?, ?>) {
        assertEquals(expectedFieldsData[i], ((LazyPrimitive<?, ?>) fieldData).getWritableObject());
      } else if (fieldData instanceof LazyHBaseCellMap) {
        assertEquals(expectedFieldsData[i], ((LazyHBaseCellMap) fieldData)
            .getMapValueElement(expectedQualifiers.get(j)).toString().trim());

        assertEquals(expectedMapSize[j], ((LazyHBaseCellMap) fieldData).getMapSize());
        // Make sure that the unwanted key is not present in the map
        assertNull(((LazyHBaseCellMap) fieldData).getMapValueElement(notPresentKey));

        j++;

      } else {
        fail("Error: field data not an instance of LazyPrimitive<?, ?> or LazyHBaseCellMap");
      }
    }

    SerDeUtils.getJSONString(row, soi);

    // Now serialize
    Put put = ((PutWritable) serDe.serialize(row, soi)).getPut();

    if (p != null) {
      assertEquals("Serialized put:", p.toString(), put.toString());
    }
  }

  @Test
  public void testHBaseSerDeCompositeKeyWithSeparator() throws SerDeException, TException,
      IOException {
    byte[] cfa = "cola".getBytes();

    byte[] qualStruct = "struct".getBytes();

    TestStruct testStruct = new TestStruct("A", "B", "C", true, (byte) 45);

    byte[] rowKey = testStruct.getBytes();

    // Data
    List<Cell> kvs = new ArrayList<Cell>();

    byte[] testData = "This is a test data".getBytes();

    kvs.add(new KeyValue(rowKey, cfa, qualStruct, testData));

    Result r = Result.create(kvs);

    Put p = new Put(rowKey);

    // Post serialization, separators are automatically inserted between different fields in the
    // struct. Currently there is not way to disable that. So the work around here is to pad the
    // data with the separator bytes before creating a "Put" object
    p.add(new KeyValue(rowKey, cfa, qualStruct, testData));

    // Create, initialize, and test the SerDe
    HBaseSerDe serDe = new HBaseSerDe();
    Configuration conf = new Configuration();
    Properties tbl = createPropertiesForCompositeKeyWithSeparator();
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);

    deserializeAndSerializeHBaseCompositeKey(serDe, r, p);
  }

  private Properties createPropertiesForCompositeKeyWithSeparator() {
    Properties tbl = new Properties();
    tbl.setProperty(serdeConstants.LIST_COLUMNS,
        "key,astring");
    tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES,
        "struct<col1:string,col2:string,col3:string>,string");
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING,
        ":key,cola:struct");
    tbl.setProperty(serdeConstants.COLLECTION_DELIM, "-");

    return tbl;
  }

  @Test
  public void testHBaseSerDeCompositeKeyWithoutSeparator() throws SerDeException, TException,
      IOException {
    byte[] cfa = "cola".getBytes();

    byte[] qualStruct = "struct".getBytes();

    TestStruct testStruct = new TestStruct("A", "B", "C", false, (byte) 0);

    byte[] rowKey = testStruct.getBytes();

    // Data
    List<Cell> kvs = new ArrayList<Cell>();

    byte[] testData = "This is a test data".getBytes();

    kvs.add(new KeyValue(rowKey, cfa, qualStruct, testData));

    Result r = Result.create(kvs);

    byte[] putRowKey = testStruct.getBytesWithDelimiters();

    Put p = new Put(putRowKey);

    // Post serialization, separators are automatically inserted between different fields in the
    // struct. Currently there is not way to disable that. So the work around here is to pad the
    // data with the separator bytes before creating a "Put" object
    p.add(new KeyValue(putRowKey, cfa, qualStruct, testData));

    // Create, initialize, and test the SerDe
    HBaseSerDe serDe = new HBaseSerDe();
    Configuration conf = new Configuration();
    Properties tbl = createPropertiesForCompositeKeyWithoutSeparator();
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);

    deserializeAndSerializeHBaseCompositeKey(serDe, r, p);
  }

  private Properties createPropertiesForCompositeKeyWithoutSeparator() {
    Properties tbl = new Properties();
    tbl.setProperty(serdeConstants.LIST_COLUMNS,
        "key,astring");
    tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES,
        "struct<col1:string,col2:string,col3:string>,string");
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING,
        ":key,cola:struct");
    tbl.setProperty(HBaseSerDe.HBASE_COMPOSITE_KEY_CLASS,
        "org.apache.hadoop.hive.hbase.HBaseTestCompositeKey");

    return tbl;
  }

  private void deserializeAndSerializeHBaseCompositeKey(HBaseSerDe serDe, Result r, Put p)
      throws SerDeException, IOException {
    StructObjectInspector soi = (StructObjectInspector) serDe.getObjectInspector();

    List<? extends StructField> fieldRefs = soi.getAllStructFieldRefs();

    Object row = serDe.deserialize(new ResultWritable(r));

    for (int j = 0; j < fieldRefs.size(); j++) {
      Object fieldData = soi.getStructFieldData(row, fieldRefs.get(j));
      assertNotNull(fieldData);
    }

    assertEquals(
        "{\"key\":{\"col1\":\"A\",\"col2\":\"B\",\"col3\":\"C\"},\"astring\":\"This is a test data\"}",
        SerDeUtils.getJSONString(row, soi));

    // Now serialize
    Put put = ((PutWritable) serDe.serialize(row, soi)).getPut();

    assertEquals("Serialized put:", p.toString(), put.toString());
  }

  @Test
  public void testHBaseSerDeWithAvroSchemaInline() throws SerDeException, IOException {
    byte[] cfa = "cola".getBytes();

    byte[] qualAvro = "avro".getBytes();

    byte[] rowKey = Bytes.toBytes("test-row1");

    // Data
    List<Cell> kvs = new ArrayList<Cell>();

    byte[] avroData = getTestAvroBytesFromSchema(RECORD_SCHEMA);

    kvs.add(new KeyValue(rowKey, cfa, qualAvro, avroData));

    Result r = Result.create(kvs);

    Put p = new Put(rowKey);

    // Post serialization, separators are automatically inserted between different fields in the
    // struct. Currently there is not way to disable that. So the work around here is to pad the
    // data with the separator bytes before creating a "Put" object
    p.add(new KeyValue(rowKey, cfa, qualAvro, avroData));

    Object[] expectedFieldsData = {new String("test-row1"), new String("[[42, true, 42432234234]]")};

    // Create, initialize, and test the SerDe
    HBaseSerDe serDe = new HBaseSerDe();
    Configuration conf = new Configuration();
    Properties tbl = createPropertiesForHiveAvroSchemaInline();
    serDe.initialize(conf, tbl);

    deserializeAndSerializeHiveAvro(serDe, r, p, expectedFieldsData,
        EXPECTED_DESERIALIZED_AVRO_STRING);
  }

  private Properties createPropertiesForHiveAvroSchemaInline() {
    Properties tbl = new Properties();
    tbl.setProperty("cola.avro.serialization.type", "avro");
    tbl.setProperty("cola.avro." + AvroTableProperties.SCHEMA_LITERAL.getPropName(), RECORD_SCHEMA);
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING, ":key,cola:avro");
    tbl.setProperty(HBaseSerDe.HBASE_AUTOGENERATE_STRUCT, "true");

    return tbl;
  }

  @Test
  public void testHBaseSerDeWithForwardEvolvedSchema() throws SerDeException, IOException {
    byte[] cfa = "cola".getBytes();

    byte[] qualAvro = "avro".getBytes();

    byte[] rowKey = Bytes.toBytes("test-row1");

    // Data
    List<Cell> kvs = new ArrayList<Cell>();

    byte[] avroData = getTestAvroBytesFromSchema(RECORD_SCHEMA);

    kvs.add(new KeyValue(rowKey, cfa, qualAvro, avroData));

    Result r = Result.create(kvs);

    Put p = new Put(rowKey);

    // Post serialization, separators are automatically inserted between different fields in the
    // struct. Currently there is not way to disable that. So the work around here is to pad the
    // data with the separator bytes before creating a "Put" object
    p.add(new KeyValue(rowKey, cfa, qualAvro, avroData));

    Object[] expectedFieldsData = {new String("test-row1"),
        new String("[[42, test, true, 42432234234]]")};

    // Create, initialize, and test the SerDe
    HBaseSerDe serDe = new HBaseSerDe();
    Configuration conf = new Configuration();
    Properties tbl = createPropertiesForHiveAvroForwardEvolvedSchema();
    serDe.initialize(conf, tbl);

    deserializeAndSerializeHiveAvro(serDe, r, p, expectedFieldsData,
        EXPECTED_DESERIALIZED_AVRO_STRING_3);
  }

  private Properties createPropertiesForHiveAvroForwardEvolvedSchema() {
    Properties tbl = new Properties();
    tbl.setProperty("cola.avro.serialization.type", "avro");
    tbl.setProperty("cola.avro." + AvroTableProperties.SCHEMA_LITERAL.getPropName(), RECORD_SCHEMA_EVOLVED);
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING, ":key,cola:avro");
    tbl.setProperty(HBaseSerDe.HBASE_AUTOGENERATE_STRUCT, "true");

    return tbl;
  }

  @Test
  public void testHBaseSerDeWithBackwardEvolvedSchema() throws SerDeException, IOException {
    byte[] cfa = "cola".getBytes();

    byte[] qualAvro = "avro".getBytes();

    byte[] rowKey = Bytes.toBytes("test-row1");

    // Data
    List<Cell> kvs = new ArrayList<Cell>();

    byte[] avroData = getTestAvroBytesFromSchema(RECORD_SCHEMA_EVOLVED);

    kvs.add(new KeyValue(rowKey, cfa, qualAvro, avroData));

    Result r = Result.create(kvs);

    Put p = new Put(rowKey);

    // Post serialization, separators are automatically inserted between different fields in the
    // struct. Currently there is not way to disable that. So the work around here is to pad the
    // data with the separator bytes before creating a "Put" object
    p.add(new KeyValue(rowKey, cfa, qualAvro, avroData));

    Object[] expectedFieldsData = {new String("test-row1"), new String("[[42, true, 42432234234]]")};

    // Create, initialize, and test the SerDe
    HBaseSerDe serDe = new HBaseSerDe();
    Configuration conf = new Configuration();
    Properties tbl = createPropertiesForHiveAvroBackwardEvolvedSchema();
    serDe.initialize(conf, tbl);

    deserializeAndSerializeHiveAvro(serDe, r, p, expectedFieldsData,
        EXPECTED_DESERIALIZED_AVRO_STRING);
  }

  private Properties createPropertiesForHiveAvroBackwardEvolvedSchema() {
    Properties tbl = new Properties();
    tbl.setProperty("cola.avro.serialization.type", "avro");
    tbl.setProperty("cola.avro." + AvroTableProperties.SCHEMA_LITERAL.getPropName(), RECORD_SCHEMA);
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING, ":key,cola:avro");
    tbl.setProperty(HBaseSerDe.HBASE_AUTOGENERATE_STRUCT, "true");

    return tbl;
  }

  @Test
  public void testHBaseSerDeWithAvroSerClass() throws SerDeException, IOException {
    byte[] cfa = "cola".getBytes();

    byte[] qualAvro = "avro".getBytes();

    byte[] rowKey = Bytes.toBytes("test-row1");

    // Data
    List<Cell> kvs = new ArrayList<Cell>();

    byte[] avroData = getTestAvroBytesFromClass1(1);

    kvs.add(new KeyValue(rowKey, cfa, qualAvro, avroData));

    Result r = Result.create(kvs);

    Put p = new Put(rowKey);

    // Post serialization, separators are automatically inserted between different fields in the
    // struct. Currently there is not way to disable that. So the work around here is to pad the
    // data with the separator bytes before creating a "Put" object
    p.add(new KeyValue(rowKey, cfa, qualAvro, avroData));

    Object[] expectedFieldsData = {
        new String("test-row1"),
        new String(
            "[Avro Employee1, 11111, 25, FEMALE, [[[Avro First Address1, Avro Second Address1, Avro City1, 123456, 0:[999, 1234567890], null, {testkey=testvalue}], "
                + "[Avro First Address1, Avro Second Address1, Avro City1, 123456, 0:[999, 1234567890], null, {testkey=testvalue}]], "
                + "[999, 1234567890], [999, 1234455555]]]")};

    // Create, initialize, and test the SerDe
    HBaseSerDe serDe = new HBaseSerDe();
    Configuration conf = new Configuration();
    Properties tbl = createPropertiesForHiveAvroSerClass();
    serDe.initialize(conf, tbl);

    deserializeAndSerializeHiveAvro(serDe, r, p, expectedFieldsData,
        EXPECTED_DESERIALIZED_AVRO_STRING_2);
  }

  private Properties createPropertiesForHiveAvroSerClass() {
    Properties tbl = new Properties();
    tbl.setProperty("cola.avro.serialization.type", "avro");
    tbl.setProperty("cola.avro." + serdeConstants.SERIALIZATION_CLASS,
        "org.apache.hadoop.hive.hbase.avro.Employee");
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING, ":key,cola:avro");
    tbl.setProperty(HBaseSerDe.HBASE_AUTOGENERATE_STRUCT, "true");

    return tbl;
  }

  @Test
  public void testHBaseSerDeWithAvroSchemaUrl() throws SerDeException, IOException {
    byte[] cfa = "cola".getBytes();

    byte[] qualAvro = "avro".getBytes();

    byte[] rowKey = Bytes.toBytes("test-row1");

    // Data
    List<Cell> kvs = new ArrayList<Cell>();

    byte[] avroData = getTestAvroBytesFromSchema(RECORD_SCHEMA);

    kvs.add(new KeyValue(rowKey, cfa, qualAvro, avroData));

    Result r = Result.create(kvs);

    Put p = new Put(rowKey);

    // Post serialization, separators are automatically inserted between different fields in the
    // struct. Currently there is not way to disable that. So the work around here is to pad the
    // data with the separator bytes before creating a "Put" object
    p.add(new KeyValue(rowKey, cfa, qualAvro, avroData));

    Object[] expectedFieldsData = {new String("test-row1"), new String("[[42, true, 42432234234]]")};

    MiniDFSCluster miniDfs = null;

    try {
      // MiniDFSCluster litters files and folders all over the place.
      miniDfs = new MiniDFSCluster(new Configuration(), 1, true, null);

      miniDfs.getFileSystem().mkdirs(new Path("/path/to/schema"));
      FSDataOutputStream out = miniDfs.getFileSystem().create(
          new Path("/path/to/schema/schema.avsc"));
      out.writeBytes(RECORD_SCHEMA);
      out.close();
      String onHDFS = miniDfs.getFileSystem().getUri() + "/path/to/schema/schema.avsc";

      // Create, initialize, and test the SerDe
      HBaseSerDe serDe = new HBaseSerDe();
      Configuration conf = new Configuration();
      Properties tbl = createPropertiesForHiveAvroSchemaUrl(onHDFS);
      serDe.initialize(conf, tbl);

      deserializeAndSerializeHiveAvro(serDe, r, p, expectedFieldsData,
          EXPECTED_DESERIALIZED_AVRO_STRING);
    } finally {
      // Teardown the cluster
      if (miniDfs != null) {
        miniDfs.shutdown();
      }
    }
  }

  private Properties createPropertiesForHiveAvroSchemaUrl(String schemaUrl) {
    Properties tbl = new Properties();
    tbl.setProperty("cola.avro.serialization.type", "avro");
    tbl.setProperty("cola.avro." + AvroTableProperties.SCHEMA_URL.getPropName(), schemaUrl);
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING, ":key,cola:avro");
    tbl.setProperty(HBaseSerDe.HBASE_AUTOGENERATE_STRUCT, "true");

    return tbl;
  }

  @Test
  public void testHBaseSerDeWithAvroExternalSchema() throws SerDeException, IOException {
    byte[] cfa = "cola".getBytes();

    byte[] qualAvro = "avro".getBytes();

    byte[] rowKey = Bytes.toBytes("test-row1");

    // Data
    List<Cell> kvs = new ArrayList<Cell>();

    byte[] avroData = getTestAvroBytesFromClass2(1);

    kvs.add(new KeyValue(rowKey, cfa, qualAvro, avroData));

    Result r = Result.create(kvs);

    Put p = new Put(rowKey);

    // Post serialization, separators are automatically inserted between different fields in the
    // struct. Currently there is not way to disable that. So the work around here is to pad the
    // data with the separator bytes before creating a "Put" object
    p.add(new KeyValue(rowKey, cfa, qualAvro, avroData));

    Object[] expectedFieldsData = {
        new String("test-row1"),
        new String(
            "[Avro Employee1, 11111, 25, FEMALE, [[[Avro First Address1, Avro Second Address1, Avro City1, 123456, 0:[999, 1234567890], null, {testkey=testvalue}], [Avro First Address1, Avro Second Address1, Avro City1, 123456, 0:[999, 1234567890], null, {testkey=testvalue}]], "
                + "[999, 1234567890], [999, 1234455555]]]")};

    // Create, initialize, and test the SerDe
    HBaseSerDe serDe = new HBaseSerDe();
    Configuration conf = new Configuration();

    Properties tbl = createPropertiesForHiveAvroExternalSchema();
    serDe.initialize(conf, tbl);

    deserializeAndSerializeHiveAvro(serDe, r, p, expectedFieldsData,
        EXPECTED_DESERIALIZED_AVRO_STRING_2);
  }

  private Properties createPropertiesForHiveAvroExternalSchema() {
    Properties tbl = new Properties();
    tbl.setProperty("cola.avro.serialization.type", "avro");
    tbl.setProperty(AvroTableProperties.SCHEMA_RETRIEVER.getPropName(),
        "org.apache.hadoop.hive.hbase.HBaseTestAvroSchemaRetriever");
    tbl.setProperty("cola.avro." + serdeConstants.SERIALIZATION_CLASS,
        "org.apache.hadoop.hive.hbase.avro.Employee");
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING, ":key,cola:avro");
    tbl.setProperty(HBaseSerDe.HBASE_AUTOGENERATE_STRUCT, "true");

    return tbl;
  }

  @Test
  public void testHBaseSerDeWithHiveMapToHBaseAvroColumnFamily() throws Exception {
    byte[] cfa = "cola".getBytes();

    byte[] qualAvroA = "prefixA_avro1".getBytes();
    byte[] qualAvroB = "prefixB_avro2".getBytes();
    byte[] qualAvroC = "prefixB_avro3".getBytes();

    List<Object> qualifiers = new ArrayList<Object>();
    qualifiers.add(new Text("prefixA_avro1"));
    qualifiers.add(new Text("prefixB_avro2"));
    qualifiers.add(new Text("prefixB_avro3"));

    List<Object> expectedQualifiers = new ArrayList<Object>();
    expectedQualifiers.add(new Text("prefixB_avro2"));
    expectedQualifiers.add(new Text("prefixB_avro3"));

    byte[] rowKey = Bytes.toBytes("test-row1");

    // Data
    List<Cell> kvs = new ArrayList<Cell>();

    byte[] avroDataA = getTestAvroBytesFromSchema(RECORD_SCHEMA);
    byte[] avroDataB = getTestAvroBytesFromClass1(1);
    byte[] avroDataC = getTestAvroBytesFromClass1(2);

    kvs.add(new KeyValue(rowKey, cfa, qualAvroA, avroDataA));
    kvs.add(new KeyValue(rowKey, cfa, qualAvroB, avroDataB));
    kvs.add(new KeyValue(rowKey, cfa, qualAvroC, avroDataC));

    Result r = Result.create(kvs);

    Put p = new Put(rowKey);

    // Post serialization, separators are automatically inserted between different fields in the
    // struct. Currently there is not way to disable that. So the work around here is to pad the
    // data with the separator bytes before creating a "Put" object
    p.add(new KeyValue(rowKey, cfa, qualAvroB, Bytes.padTail(avroDataB, 11)));
    p.add(new KeyValue(rowKey, cfa, qualAvroC, Bytes.padTail(avroDataC, 11)));

    Object[] expectedFieldsData = {
        new Text("test-row1"),
        new String(
            "[Avro Employee1, 11111, 25, FEMALE, [[[Avro First Address1, Avro Second Address1, Avro City1, 123456, 0:[999, 1234567890], null, {testkey=testvalue}], [Avro First Address1, Avro Second Address1, Avro City1, 123456, 0:[999, 1234567890], null, {testkey=testvalue}]], "
                + "[999, 1234567890], [999, 1234455555]]]"),
        new String(
            "[Avro Employee2, 11111, 25, FEMALE, [[[Avro First Address2, Avro Second Address2, Avro City2, 123456, 0:[999, 1234567890], null, {testkey=testvalue}], [Avro First Address2, Avro Second Address2, Avro City2, 123456, 0:[999, 1234567890], null, {testkey=testvalue}]], "
                + "[999, 1234567890], [999, 1234455555]]]")};

    int[] expectedMapSize = new int[] {2};

    // Create, initialize, and test the SerDe
    HBaseSerDe serDe = new HBaseSerDe();
    Configuration conf = new Configuration();
    Properties tbl = createPropertiesForHiveAvroColumnFamilyMap();
    serDe.initialize(conf, tbl);

    Object notPresentKey = new Text("prefixA_avro1");

    deserializeAndSerializeHiveStructColumnFamily(serDe, r, p, expectedFieldsData, expectedMapSize,
        expectedQualifiers,
        notPresentKey);
  }

  private Properties createPropertiesForHiveAvroColumnFamilyMap() {
    Properties tbl = new Properties();
    tbl.setProperty("cola.prefixB_.serialization.type", "avro");
    tbl.setProperty("cola.prefixB_." + serdeConstants.SERIALIZATION_CLASS,
        "org.apache.hadoop.hive.hbase.avro.Employee");
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING, "cola:prefixB_.*");
    tbl.setProperty(HBaseSerDe.HBASE_AUTOGENERATE_STRUCT, "true");
    tbl.setProperty(LazySerDeParameters.SERIALIZATION_EXTEND_NESTING_LEVELS, "true");

    return tbl;
  }

  @Test
  public void testHBaseSerDeCustomStructValue() throws IOException, SerDeException {

    byte[] cfa = "cola".getBytes();
    byte[] qualStruct = "struct".getBytes();

    TestStruct testStruct = new TestStruct("A", "B", "C", false, (byte) 0);
    byte[] key = testStruct.getBytes();
    // Data
    List<Cell> kvs = new ArrayList<Cell>();

    byte[] testData = testStruct.getBytes();
    kvs.add(new KeyValue(key, cfa, qualStruct, testData));

    Result r = Result.create(kvs);
    byte[] putKey = testStruct.getBytesWithDelimiters();

    Put p = new Put(putKey);

    // Post serialization, separators are automatically inserted between different fields in the
    // struct. Currently there is not way to disable that. So the work around here is to pad the
    // data with the separator bytes before creating a "Put" object
    p.add(new KeyValue(putKey, cfa, qualStruct, Bytes.padTail(testData, 2)));

    // Create, initialize, and test the SerDe
    HBaseSerDe serDe = new HBaseSerDe();
    Configuration conf = new Configuration();
    Properties tbl = createPropertiesForValueStruct();
    serDe.initialize(conf, tbl);

    deserializeAndSerializeHBaseValueStruct(serDe, r, p);

  }

  /**
   * Since there are assertions in the code, when running this test it throws an assertion error
   * and not the error in a production setup. The Properties.java object that is passed to the serDe
   * initializer, is passed with empty value "" for "columns.comments" key for hbase backed tables.
   */
  @Test
  public void testEmptyColumnComment() throws SerDeException {
    HBaseSerDe serDe = new HBaseSerDe();
    Properties tbl = createPropertiesForValueStruct();
    tbl.setProperty("columns.comments", "");
    serDe.initialize(new Configuration(), tbl);
  }

  private Properties createPropertiesForValueStruct() {
    Properties tbl = new Properties();
    tbl.setProperty("cola.struct.serialization.type", "struct");
    tbl.setProperty("cola.struct.test.value", "test value");
    tbl.setProperty(HBaseSerDe.HBASE_STRUCT_SERIALIZER_CLASS,
        "org.apache.hadoop.hive.hbase.HBaseTestStructSerializer");
    tbl.setProperty(serdeConstants.LIST_COLUMNS, "key,astring");
    tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES,
        "struct<col1:string,col2:string,col3:string>,struct<col1:string,col2:string,col3:string>");
    tbl.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING, ":key,cola:struct");
    tbl.setProperty(HBaseSerDe.HBASE_COMPOSITE_KEY_CLASS,
        "org.apache.hadoop.hive.hbase.HBaseTestCompositeKey");
    return tbl;
  }

  private void deserializeAndSerializeHBaseValueStruct(HBaseSerDe serDe, Result r, Put p)
      throws SerDeException, IOException {
    StructObjectInspector soi = (StructObjectInspector) serDe.getObjectInspector();

    List<? extends StructField> fieldRefs = soi.getAllStructFieldRefs();

    Object row = serDe.deserialize(new ResultWritable(r));

    Object fieldData = null;
    for (int j = 0; j < fieldRefs.size(); j++) {
      fieldData = soi.getStructFieldData(row, fieldRefs.get(j));
      assertNotNull(fieldData);
      if (fieldData instanceof LazyStruct) {
        assertEquals(((LazyStruct) fieldData).getField(0).toString(), "A");
        assertEquals(((LazyStruct) fieldData).getField(1).toString(), "B");
        assertEquals(((LazyStruct) fieldData).getField(2).toString(), "C");
      } else {
        Assert.fail("fieldData should be an instance of LazyStruct");
      }
    }

    assertEquals(
        "{\"key\":{\"col1\":\"A\",\"col2\":\"B\",\"col3\":\"C\"},\"astring\":{\"col1\":\"A\",\"col2\":\"B\",\"col3\":\"C\"}}",
        SerDeUtils.getJSONString(row, soi));

    // Now serialize
    Put put = ((PutWritable) serDe.serialize(row, soi)).getPut();

    assertEquals("Serialized put:", p.toString(), put.toString());
  }

  private void deserializeAndSerializeHiveAvro(HBaseSerDe serDe, Result r, Put p,
      Object[] expectedFieldsData, String expectedDeserializedAvroString)
      throws SerDeException, IOException {
    StructObjectInspector soi = (StructObjectInspector) serDe.getObjectInspector();

    List<? extends StructField> fieldRefs = soi.getAllStructFieldRefs();

    Object row = serDe.deserialize(new ResultWritable(r));

    for (int j = 0; j < fieldRefs.size(); j++) {
      Object fieldData = soi.getStructFieldData(row, fieldRefs.get(j));
      assertNotNull(fieldData);
      assertEquals(expectedFieldsData[j], fieldData.toString().trim());
    }

    assertEquals(expectedDeserializedAvroString, SerDeUtils.getJSONString(row, soi));

    // Now serialize
    Put put = ((PutWritable) serDe.serialize(row, soi)).getPut();

    assertNotNull(put);
    assertEquals(p.getFamilyCellMap(), put.getFamilyCellMap());
  }

  private void deserializeAndSerializeHiveStructColumnFamily(HBaseSerDe serDe, Result r, Put p,
      Object[] expectedFieldsData,
      int[] expectedMapSize, List<Object> expectedQualifiers, Object notPresentKey)
      throws SerDeException, IOException {
    StructObjectInspector soi = (StructObjectInspector) serDe.getObjectInspector();

    List<? extends StructField> fieldRefs = soi.getAllStructFieldRefs();

    Object row = serDe.deserialize(new ResultWritable(r));

    int k = 0;

    for (int i = 0; i < fieldRefs.size(); i++) {
      Object fieldData = soi.getStructFieldData(row, fieldRefs.get(i));
      assertNotNull(fieldData);

      if (fieldData instanceof LazyPrimitive<?, ?>) {
        assertEquals(expectedFieldsData[i], ((LazyPrimitive<?, ?>) fieldData).getWritableObject());
      } else if (fieldData instanceof LazyHBaseCellMap) {

        for (int j = 0; j < ((LazyHBaseCellMap) fieldData).getMapSize(); j++) {
          assertEquals(expectedFieldsData[k + 1],
              ((LazyHBaseCellMap) fieldData).getMapValueElement(expectedQualifiers.get(k))
                  .toString().trim());
          k++;
        }

        assertEquals(expectedMapSize[i - 1], ((LazyHBaseCellMap) fieldData).getMapSize());

        // Make sure that the unwanted key is not present in the map
        assertNull(((LazyHBaseCellMap) fieldData).getMapValueElement(notPresentKey));

      } else {
        fail("Error: field data not an instance of LazyPrimitive<?, ?> or LazyHBaseCellMap");
      }
    }

    SerDeUtils.getJSONString(row, soi);

    // Now serialize
    Put put = ((PutWritable) serDe.serialize(row, soi)).getPut();

    assertNotNull(put);
  }

  private byte[] getTestAvroBytesFromSchema(String schemaToUse) throws IOException {
    Schema s = Schema.parse(schemaToUse);
    GenericData.Record record = new GenericData.Record(s);
    GenericData.Record innerRecord = new GenericData.Record(s.getField("aRecord").schema());
    innerRecord.put("int1", 42);
    innerRecord.put("boolean1", true);
    innerRecord.put("long1", 42432234234l);

    if (schemaToUse.equals(RECORD_SCHEMA_EVOLVED)) {
      innerRecord.put("string1", "new value");
    }

    record.put("aRecord", innerRecord);

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(s);
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    dataFileWriter.create(s, out);
    dataFileWriter.append(record);
    dataFileWriter.close();

    byte[] data = out.toByteArray();

    out.close();
    return data;
  }

  private byte[] getTestAvroBytesFromClass1(int i) throws IOException {
    Employee employee = new Employee();

    employee.setEmployeeName("Avro Employee" + i);
    employee.setEmployeeID(11111L);
    employee.setGender(Gender.FEMALE);
    employee.setAge(25L);

    Address address = new Address();

    address.setAddress1("Avro First Address" + i);
    address.setAddress2("Avro Second Address" + i);
    address.setCity("Avro City" + i);
    address.setZipcode(123456L);

    Map<CharSequence, CharSequence> metadata = new HashMap<CharSequence, CharSequence>();

    metadata.put("testkey", "testvalue");

    address.setMetadata(metadata);

    HomePhone hPhone = new HomePhone();

    hPhone.setAreaCode(999L);
    hPhone.setNumber(1234567890L);

    OfficePhone oPhone = new OfficePhone();

    oPhone.setAreaCode(999L);
    oPhone.setNumber(1234455555L);

    ContactInfo contact = new ContactInfo();

    List<Address> addresses = new ArrayList<Address>();
    address.setCounty(hPhone); // set value for the union type
    addresses.add(address);
    addresses.add(address);

    contact.setAddress(addresses);

    contact.setHomePhone(hPhone);
    contact.setOfficePhone(oPhone);

    employee.setContactInfo(contact);

    DatumWriter<Employee> datumWriter = new SpecificDatumWriter<Employee>(Employee.class);
    DataFileWriter<Employee> dataFileWriter = new DataFileWriter<Employee>(datumWriter);

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    dataFileWriter.create(employee.getSchema(), out);
    dataFileWriter.append(employee);
    dataFileWriter.close();

    return out.toByteArray();
  }

  private byte[] getTestAvroBytesFromClass2(int i) throws IOException {
    Employee employee = new Employee();

    employee.setEmployeeName("Avro Employee" + i);
    employee.setEmployeeID(11111L);
    employee.setGender(Gender.FEMALE);
    employee.setAge(25L);

    Address address = new Address();

    address.setAddress1("Avro First Address" + i);
    address.setAddress2("Avro Second Address" + i);
    address.setCity("Avro City" + i);
    address.setZipcode(123456L);

    Map<CharSequence, CharSequence> metadata = new HashMap<CharSequence, CharSequence>();

    metadata.put("testkey", "testvalue");

    address.setMetadata(metadata);

    HomePhone hPhone = new HomePhone();

    hPhone.setAreaCode(999L);
    hPhone.setNumber(1234567890L);

    OfficePhone oPhone = new OfficePhone();

    oPhone.setAreaCode(999L);
    oPhone.setNumber(1234455555L);

    ContactInfo contact = new ContactInfo();

    List<Address> addresses = new ArrayList<Address>();
    address.setCounty(hPhone); // set value for the union type
    addresses.add(address);
    addresses.add(address);

    contact.setAddress(addresses);

    contact.setHomePhone(hPhone);
    contact.setOfficePhone(oPhone);

    employee.setContactInfo(contact);

    DatumWriter<Employee> employeeWriter = new SpecificDatumWriter<Employee>(Employee.class);

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);

    // write out a header for the payload
    out.write(TEST_BYTE_ARRAY);

    employeeWriter.write(employee, encoder);

    encoder.flush();

    return out.toByteArray();
  }

  class TestStruct {
    String f1;
    String f2;
    String f3;
    boolean hasSeparator;
    byte separator;

    TestStruct(String f1, String f2, String f3, boolean hasSeparator, byte separator) {
      this.f1 = f1;
      this.f2 = f2;
      this.f3 = f3;
      this.hasSeparator = hasSeparator;
      this.separator = separator;
    }

    public byte[] getBytes() throws IOException {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();

      bos.write(f1.getBytes());
      if (hasSeparator) {
        bos.write(separator); // Add field separator
      }
      bos.write(f2.getBytes());
      if (hasSeparator) {
        bos.write(separator);
      }
      bos.write(f3.getBytes());

      return bos.toByteArray();
    }

    public byte[] getBytesWithDelimiters() throws IOException {
      // Add Ctrl-B delimiter between the fields. This is necessary because for structs in case no
      // delimiter is provided, hive automatically adds Ctrl-B as a default delimiter between fields
      ByteArrayOutputStream bos = new ByteArrayOutputStream();

      bos.write(f1.getBytes());
      bos.write("\002".getBytes("UTF8"));
      bos.write(f2.getBytes());
      bos.write("\002".getBytes("UTF8"));
      bos.write(f3.getBytes());

      return bos.toByteArray();
    }
  }
}
