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

package org.apache.hadoop.hive.serde2;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.TimeZone;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test suite for the JSON SerDe class.
 */
public class TestJsonSerDe {

  @Test
  public void testPrimitiveDataTypes() throws Exception {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS,
        "name,height,weight,endangered,born");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES,
        "string,float,int,boolean,timestamp");
    props.setProperty(serdeConstants.TIMESTAMP_FORMATS, "millis");

    JsonSerDe serde = new JsonSerDe();
    serde.initialize(new Configuration(), props, null, false);

    final String jsonText = loadJson("simple.json");

    final Text text = new Text(jsonText);
    final List<?> results = (List<?>) serde.deserialize(text);

    Assert.assertNotNull(results);
    Assert.assertEquals(5, results.size());
    Assert.assertEquals("giraffe", results.get(0));
    Assert.assertEquals(5.5f, results.get(1));
    Assert.assertEquals(1360, results.get(2));
    Assert.assertEquals(true, results.get(3));
    Assert.assertEquals(Timestamp.ofEpochMilli(1549751270013L), results.get(4));
  }

  @Test
  public void testComplexType() throws Exception {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS,
        "__dc_timelabel," + "__dc_load_time," + "id," + "name," + "location," + "primary_contact_user_id,"
            + "parent_id," + "parent_office_external_id," + "child_ids," + "child_office_external_ids," +"external_id");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES,
        "timestamp," + "timestamp," + "bigint," + "string," + "string," + "bigint," + "bigint," + "string," + "string,"
            + "string," + "string");
    props.setProperty(serdeConstants.TIMESTAMP_FORMATS, "yyyy-MM-ddHH:mm:ss");

    JsonSerDe serde = new JsonSerDe();
    serde.initialize(new Configuration(), props, null, false);

    final String jsonText = loadJson("nested_sample_1.json");
    final Text text = new Text(jsonText);
    final List<?> results = (List<?>) serde.deserialize(text);

    Assert.assertEquals(11, results.size());
    Assert.assertEquals("Brooklyn-200", results.get(3));
    // make sure inner struct can be decoded
    Assert.assertEquals("{\"name\":\"Brooklyn,NY\"}", results.get(4));
  }

  @Test
  public void testDisabledComplexType() throws Exception {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS,
        "__dc_timelabel," + "__dc_load_time," + "id," + "name," + "location," + "primary_contact_user_id,"
            + "parent_id," + "parent_office_external_id," + "child_ids," + "child_office_external_ids," +"external_id");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES,
        "timestamp," + "timestamp," + "bigint," + "string," + "string," + "bigint," + "bigint," + "string," + "string,"
            + "string," + "string");
    props.setProperty(serdeConstants.TIMESTAMP_FORMATS, "yyyy-MM-ddHH:mm:ss");
    props.setProperty(JsonSerDe.STRINGIFY_COMPLEX, "false");

    JsonSerDe serde = new JsonSerDe();
    serde.initialize(new Configuration(), props, null, false);

    final String jsonText = loadJson("nested_sample_1.json");
    final Text text = new Text(jsonText);

    Exception exception = Assert.assertThrows(SerDeException.class, () -> serde.deserialize(text));
    String expectedMessage = "Complex field found in JSON does not match table definition: string";
    String actualMessage = exception.getMessage();
    Assert.assertTrue(actualMessage.contains(expectedMessage));
  }

  @Test
  public void testMoreComplexType() throws Exception {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, "data," + "messageId," + "publish_time," + "attributes");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, "string," + "string," + "bigint," + "string");

    JsonSerDe serde = new JsonSerDe();
    serde.initialize(new Configuration(), props, null, false);
    final String jsonText = loadJson("nested_sample_2.json");

    final Text text = new Text(jsonText);
    final List<?> results = (List<?>) serde.deserialize(text);

    Assert.assertEquals(4, results.size());
    Assert.assertEquals("{\"H\":{\"event\":\"track_active\",\"platform\":\"Android\"},"
        + "\"B\":{\"device_type\":\"Phone\",\"uuid\":"
        + "\"[36ffec24-f6a4-4f5d-aa39-72e5513d2cae,11883bee-a7aa-4010-8a66-6c3c63a73f16]\"}}", results.get(0));
    Assert.assertEquals("2475185636801962", results.get(1));
    Assert.assertEquals(1622514629783L, results.get(2));
    Assert.assertEquals("{\"region\":\"IN\"}", results.get(3));
  }

  @Test
  public void testArray() throws Exception {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, "list,items");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, "string,array<string>");
    props.setProperty(serdeConstants.TIMESTAMP_FORMATS, "millis");

    JsonSerDe serde = new JsonSerDe();
    serde.initialize(new Configuration(), props, null, false);

    final String jsonText = loadJson("array.json");

    final Text text = new Text(jsonText);
    final List<?> results = (List<?>) serde.deserialize(text);

    Assert.assertNotNull(results);
    Assert.assertEquals(2, results.size());
    Assert.assertEquals("grocery", results.get(0));
    Assert.assertEquals(Arrays.asList("milk", "eggs", "bread"), results.get(1));
  }

  /**
   * Test when a map has a key defined as a numeric value. Technically, JSON
   * does not support this because each key in a map must be a quoted string.
   * Unquoted strings (hence an int value) is allowed by JavaScript, but not by
   * JSON specification. For Hive, the int map key type is stored as a string
   * and must be converted back into an int type.
   */
  @Test
  public void testMapNumericKey() throws Exception {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, "map");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, "map<int,string>");
    props.setProperty(serdeConstants.TIMESTAMP_FORMATS, "millis");

    JsonSerDe serde = new JsonSerDe();
    serde.initialize(new Configuration(), props, null, false);

    final String jsonText = loadJson("map_int_key.json");

    final Text text = new Text(jsonText);
    final List<?> results = (List<?>) serde.deserialize(text);

    Assert.assertNotNull(results);
    Assert.assertEquals(1, results.size());

    Map<?, ?> resultMap = (Map<?, ?>) results.get(0);
    Object value1 = resultMap.get(1);
    Object value2 = resultMap.get(2);

    Assert.assertEquals(2, resultMap.size());
    Assert.assertEquals("2001-01-01", value1);
    Assert.assertEquals(null, value2);
  }

  @Test
  public void testBlankLineAllowed() throws Exception {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, "a,b,c");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, "int,int,int");
    props.setProperty(serdeConstants.TIMESTAMP_FORMATS, "millis");

    // This is the test parameter
    props.setProperty(JsonSerDe.NULL_EMPTY_LINES, "true");

    JsonSerDe serde = new JsonSerDe();
    serde.initialize(new Configuration(), props, null, false);

    final Text text = new Text("");
    final List<?> results = (List<?>) serde.deserialize(text);
    Assert.assertEquals(Arrays.asList(null, null, null), results);
  }

  @Test(expected = SerDeException.class)
  public void testBlankLineException() throws Exception {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, "a,b,c");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, "int,int,int");
    props.setProperty(serdeConstants.TIMESTAMP_FORMATS, "millis");

    // This is the test parameter
    props.setProperty(JsonSerDe.NULL_EMPTY_LINES, "false");

    JsonSerDe serde = new JsonSerDe();
    serde.initialize(new Configuration(), props, null, false);

    serde.deserialize(new Text(""));
  }

  @Test
  public void testChar() throws Exception {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, "a");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, "char(5)");
    props.setProperty(serdeConstants.TIMESTAMP_FORMATS, "millis");

    JsonSerDe serde = new JsonSerDe();
    serde.initialize(new Configuration(), props, null, false);

    List<?> results = (List<?>) serde.deserialize(new Text("{\"a\":\"xxx\"}"));
    Assert.assertNotNull(results);
    Assert.assertEquals(1, results.size());
    Assert.assertEquals("xxx  ", results.get(0).toString());
  }

  /**
   * When parsing the JSON object, a cache is kept for the definition of each
   * field and it index in its most immediate struct. Check that if two names
   * have the same name, in the same index of their respective structs, that
   * they are not confused with one-another.
   */
  @Test
  public void testCacheIndexSameFieldName() throws Exception {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, "a,b");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES,
        "int,struct<a:boolean>");
    props.setProperty(serdeConstants.TIMESTAMP_FORMATS, "millis");

    JsonSerDe serde = new JsonSerDe();
    serde.initialize(new Configuration(), props, null, false);

    List<?> results =
        (List<?>) serde.deserialize(new Text("{\"a\":5,\"b\":{\"a\":true}}"));
    Assert.assertNotNull(results);
    Assert.assertEquals(2, results.size());
    Assert.assertEquals(5, results.get(0));
    Assert.assertEquals(Arrays.asList(true), results.get(1));
  }

  @Test
  public void testSerialize() throws SerDeException, IOException {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, "a,b,c,d,e,f");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES,
        "int,int,struct<c:int,d:int>,string,array<int>,map<int,float>");
    props.setProperty(serdeConstants.TIMESTAMP_FORMATS, "millis");

    final String jsonText = loadJson("complex_write.json");

    JsonSerDe serde = new JsonSerDe();
    serde.initialize(new Configuration(), props, null, false);

    ObjectInspector oi = serde.getObjectInspector();

    Map<IntWritable, FloatWritable> map = new LinkedHashMap<>();
    map.put(new IntWritable(666), new FloatWritable(0.12345f));
    map.put(new IntWritable(999), new FloatWritable(0.6789f));

    Object testStructure = Arrays.asList(new IntWritable(2), new IntWritable(3),
        Arrays.asList(new IntWritable(2), new IntWritable(3)),
        new Text("SomeData"),
        Arrays.asList(new IntWritable(101), new IntWritable(102)), map);

    Object result = serde.serialize(testStructure, oi);
    Text text = (Text) result;
    Assert.assertEquals(jsonText, text.toString());
  }

  /**
   * Test serializing a information about a file to JSON. The name of the file
   * is a "single_pixel.png" and the binary data of the PNG file are stored in
   * JSON. By default, the writer uses base-64 mime encoding.
   */
  @Test
  public void testSerializeBinaryData() throws Exception {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, "name,content");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, "string,binary");
    props.setProperty(serdeConstants.TIMESTAMP_FORMATS, "millis");

    final String jsonText = loadJson("single_pixel.json");

    JsonSerDe serde = new JsonSerDe();
    serde.initialize(new Configuration(), props, null, false);

    ObjectInspector oi = serde.getObjectInspector();

    final byte[] buf = Base64.getMimeDecoder()
        .decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAf"
            + "FcSJAAAADUlEQVR42mNUuXDhPwAFqQLFTWHXeAAAAABJRU5ErkJggg==");

    Object testObject =
        Arrays.asList(new Text("single_pixel.png"), new BytesWritable(buf));

    Object result = serde.serialize(testObject, oi);
    Text text = (Text) result;
    Assert.assertEquals(jsonText, text.toString());
  }

  /**
   * Test serializing "timestamp with local time zone". Take a time in GMT and
   * have it convert it to the local time.
   */
  @Test
  public void testTimestampWithLocalTimeZone() throws Exception {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, "__time");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES,
        "timestamp with local time zone");
    props.setProperty(serdeConstants.TIMESTAMP_FORMATS,
        "yyyy-MM-dd'T'HH:mm:ss'Z'");

    final TimeZone localTz = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("US/Pacific"));

    try {
      JsonSerDe serde = new JsonSerDe();
      serde.initialize(new Configuration(), props, null, false);

      List<?> results = (List<?>) serde
          .deserialize(new Text("{\"__time\":\"2013-08-31T01:02:33Z\"}"));

      Assert.assertNotNull(results);
      Assert.assertEquals(1, results.size());
      Assert.assertTrue(results.get(0) instanceof TimestampTZ);
      Assert.assertEquals("2013-08-30 18:02:33.0 US/Pacific",
          results.get(0).toString());

    } finally {
      TimeZone.setDefault(localTz);
    }
  }

  /**
   * Accepts a file name and loads it from /src/test/resource/json
   *
   * @param resourceName The name of the file to load
   * @return A JSON string, all whitespace removed
   * @throws IOException Failed to load the file
   */
  private String loadJson(final String resourceName) throws IOException {
    final String path = "/json/" + resourceName;
    final URL url = this.getClass().getResource(path);
    final File testJson = new File(url.getFile());
    final String jsonText =
        FileUtils.readFileToString(testJson, StandardCharsets.UTF_8);
    return jsonText.replaceAll("\\s+", "");
  }

  /**
   * As HIVE-27180 removes hcatalog JsonSerDe, Moved tests from hcatalog/data/TestJsonSerDe
   * here to ensure the compatibility with new serde2 JsonSerDe implementation.
   */
  public List<Pair<Properties, List<Object>>> getData() throws UnsupportedEncodingException {
    ArrayList<Pair<Properties, List<Object>>> dataPairs = new ArrayList<>();

    List<Object> rlist = new ArrayList<Object>(13);
    rlist.add(new ByteWritable((byte) 123));
    rlist.add(new ShortWritable((short) 456));
    rlist.add(new IntWritable(789));
    rlist.add(new LongWritable(1000L));
    rlist.add(new DoubleWritable(5.3D));
    rlist.add(new FloatWritable(2.39F));
    rlist.add(new Text("hcatandhadoop"));
    rlist.add(null);

    List<Object> innerStruct = new ArrayList<Object>(2);
    innerStruct.add(new Text("abc"));
    innerStruct.add(new Text("def")) ;
    rlist.add(innerStruct);

    List<IntWritable> innerList = new ArrayList<IntWritable>();
    innerList.add(new IntWritable(314));
    innerList.add(new IntWritable(007));
    rlist.add(innerList);

    rlist.add(new BooleanWritable(true));

    List<Object> c1 = new ArrayList<Object>();
    List<Object> c1_1 = new ArrayList<Object>();
    c1_1.add(new IntWritable(12));
    List<Object> i2 = new ArrayList<Object>();
    List<IntWritable> ii1 = new ArrayList<IntWritable>();
    ii1.add(new IntWritable(13));
    ii1.add(new IntWritable(14));
    i2.add(ii1);
    Map<Text, List<?>> ii2 = new HashMap<Text, List<?>>();
    List<IntWritable> iii1 = new ArrayList<IntWritable>();
    iii1.add(new IntWritable(15));
    ii2.put(new Text("phew"), iii1);
    i2.add(ii2);
    c1_1.add(i2);
    c1.add(c1_1);
    rlist.add(c1);
    rlist.add(new HiveDecimalWritable(HiveDecimal.create(new BigDecimal("123.45"))));//prec 5, scale 2
    rlist.add(new HiveCharWritable("hivechar".getBytes(), 10));
    rlist.add(new HiveVarcharWritable("hivevarchar".getBytes(), 20));
    rlist.add(new DateWritableV2(Date.valueOf("2014-01-07")));

    List<Object> nlist = new ArrayList<Object>(13);
    nlist.add(null); // tinyint
    nlist.add(null); // smallint
    nlist.add(null); // int
    nlist.add(null); // bigint
    nlist.add(null); // double
    nlist.add(null); // float
    nlist.add(null); // string
    nlist.add(null); // string
    nlist.add(null); // struct
    nlist.add(null); // array
    nlist.add(null); // bool
    nlist.add(null); // complex
    nlist.add(null); //decimal(5,2)
    nlist.add(null); //char(10)
    nlist.add(null); //varchar(20)
    nlist.add(null); //date

    Properties props = new Properties();

    String typeString =
            "tinyint,smallint,int,bigint,double,float,string,string,"
                    + "struct<a:string,b:string>,array<int>,boolean,"
                    + "array<struct<i1:int,i2:struct<ii1:array<int>,ii2:map<string,struct<iii1:int>>>>>," +
                    "decimal(5,2),char(10),varchar(20),date";

    props.put(serdeConstants.LIST_COLUMNS, "ti,si,i,bi,d,f,s,n,r,l,b,c1,bd,hc,hvc,dt");
    props.put(serdeConstants.LIST_COLUMN_TYPES, typeString);

    dataPairs.add(Pair.of(props, rlist));
    dataPairs.add(Pair.of(props, nlist));
    return dataPairs;
  }

  @Test
  public void testRW() throws Exception {

    Configuration conf = new Configuration();
    JsonParser parser = new JsonParser();


    for (Pair<Properties, List<Object>> e : getData()) {
      Properties tblProps = e.getLeft();
      List<Object> testObject = e.getRight();

      JsonSerDe serde = new JsonSerDe();
      serde.initialize(conf, tblProps, null,false);
      ObjectInspector oi = serde.getObjectInspector();

      Writable s2 = serde.serialize(testObject, oi);
      JsonElement o1 = parser.parse(testObject.toString());
      JsonElement o2 = parser.parse(serde.deserialize(s2).toString());
      assertEquals(o1,o2);
    }
  }

  @Test
  public void testRobustRead() throws Exception {
    /**
     *  This test has been added to account for HCATALOG-436
     *  We write out columns with "internal column names" such
     *  as "_col0", but try to read with regular column names.
     */

    Configuration conf = new Configuration();
    JsonParser parser = new JsonParser();

    for (Pair<Properties, List<Object>> e : getData()) {
      Properties tblProps = e.getLeft();
      List<Object> testObject = e.getRight();

      Properties internalTblProps = new Properties();
      for (Map.Entry pe : tblProps.entrySet()) {
        if (!pe.getKey().equals(serdeConstants.LIST_COLUMNS)) {
          internalTblProps.put(pe.getKey(), pe.getValue());
        } else {
          internalTblProps.put(pe.getKey(), getInternalNames((String) pe.getValue()));
        }
      }
      JsonSerDe wjsd = new JsonSerDe();
      wjsd.initialize(conf, internalTblProps, null,false);

      JsonSerDe rjsd = new JsonSerDe();
      rjsd.initialize(conf, tblProps, null,false);
      Writable s = wjsd.serialize(testObject, wjsd.getObjectInspector());

      JsonElement o3 = parser.parse(testObject.toString());
      JsonElement o4 = parser.parse(rjsd.deserialize(s).toString());
      assertEquals(o3,o4);
    }

  }

  String getInternalNames(String columnNames) {
    if (columnNames == null) {
      return null;
    }
    if (columnNames.isEmpty()) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    int numStrings = columnNames.split(",").length;
    sb.append("_col0");
    for (int i = 1; i < numStrings; i++) {
      sb.append(",");
      sb.append(HiveConf.getColumnInternalName(i));
    }
    return sb.toString();
  }

  /**
   * This test tests that our json deserialization is not too strict, as per HIVE-6166
   * <p>
   * i.e, if our schema is "s:struct<a:int,b:string>,k:int", and we pass in
   * data that looks like : {
   * "x" : "abc" ,
   * "t" : {
   * "a" : "1",
   * "b" : "2",
   * "c" : [
   * { "x" : 2 , "y" : 3 } ,
   * { "x" : 3 , "y" : 2 }
   * ]
   * } ,
   * "s" : {
   * "a" : 2 ,
   * "b" : "blah",
   * "c": "woo"
   * }
   * }
   * <p>
   * Then it should still work, and ignore the "x" and "t" field and "c" subfield of "s", and it
   * should read k as null.
   */
  @Test
  public void testLooseJsonReadability() throws Exception {
    Configuration conf = new Configuration();
    Properties props = new Properties();
    JsonParser parser = new JsonParser();

    props.put(serdeConstants.LIST_COLUMNS, "s,k");
    props.put(serdeConstants.LIST_COLUMN_TYPES, "struct<a:int,b:string>,int");
    JsonSerDe rjsd = new JsonSerDe();
    rjsd.initialize(conf, props, null);

    Text jsonText = new Text("{ \"x\" : \"abc\" , "
            + " \"t\" : { \"a\":\"1\", \"b\":\"2\", \"c\":[ { \"x\":2 , \"y\":3 } , { \"x\":3 , \"y\":2 }] } ,"
            + "\"s\" : { \"a\" : 2 , \"b\" : \"blah\", \"c\": \"woo\" } }");
    List<Object> expected = new ArrayList<Object>();
    List<Object> inner = new ArrayList<Object>();
    inner.add(2);
    inner.add("blah");
    expected.add(inner);
    expected.add(null);
    JsonElement o1 = parser.parse(expected.toString());
    JsonElement o2 = parser.parse(rjsd.deserialize(jsonText).toString());

    assertEquals(o1,o2);

  }

  @Test
  public void testUpperCaseKey() throws Exception {
    Configuration conf = new Configuration();
    Properties props = new Properties();
    JsonParser parser = new JsonParser();

    props.put(serdeConstants.LIST_COLUMNS, "empid,name");
    props.put(serdeConstants.LIST_COLUMN_TYPES, "int,string");
    JsonSerDe rjsd = new JsonSerDe();
    rjsd.initialize(conf, props, null,false);

    Text text1 = new Text("{ \"empId\" : 123, \"name\" : \"John\" } ");
    Text text2 = new Text("{ \"empId\" : 456, \"name\" : \"Jane\" } ");

    String expected1 = Arrays.<Object>asList(123, "John").toString();
    String expected2 = Arrays.<Object>asList(456, "Jane").toString();

    JsonElement o1 = parser.parse(expected1);
    JsonElement o2 = parser.parse(rjsd.deserialize(text1).toString());
    JsonElement o3 = parser.parse(expected2);
    JsonElement o4 = parser.parse(rjsd.deserialize(text2).toString());

    assertEquals(o1,o2);
    assertEquals(o3,o4);

  }

  private static HashMap<String, Integer> createHashMapStringInteger(Object... vals) {
    assertTrue(vals.length % 2 == 0);
    HashMap<String, Integer> retval = new HashMap<String, Integer>();
    for (int idx = 0; idx < vals.length; idx += 2) {
      retval.put((String) vals[idx], (Integer) vals[idx + 1]);
    }
    return retval;
  }

  @Test
  public void testMapValues() throws Exception {
    Configuration conf = new Configuration();
    Properties props = new Properties();
    JsonParser parser = new JsonParser();

    props.put(serdeConstants.LIST_COLUMNS, "a,b");
    props.put(serdeConstants.LIST_COLUMN_TYPES, "array<string>,map<string,int>");
    JsonSerDe rjsd = new JsonSerDe();
    rjsd.initialize(conf, props, null,false);

    Text text1 = new Text("{ \"a\":[\"aaa\"],\"b\":{\"bbb\":1}} ");
    Text text2 = new Text("{\"a\":[\"yyy\"],\"b\":{\"zzz\":123}}");
    Text text3 = new Text("{\"a\":[\"a\"],\"b\":{\"x\":11, \"y\": 22, \"z\": null}}");

    String expected1 = Arrays.<Object>asList(
            Arrays.<String>asList("aaa"),
            createHashMapStringInteger("bbb", 1)).toString();
    String expected2 = Arrays.<Object>asList(
            Arrays.<String>asList("yyy"),
            createHashMapStringInteger("zzz", 123)).toString();
    String expected3 = Arrays.<Object>asList(
            Arrays.<String>asList("a"),
            createHashMapStringInteger("x", 11, "y", 22, "z", null)).toString();
    JsonElement o1 = parser.parse(expected1);
    JsonElement o2 = parser.parse(rjsd.deserialize(text1).toString());
    JsonElement o3 = parser.parse(expected2);
    JsonElement o4 = parser.parse(rjsd.deserialize(text2).toString());
    JsonElement o5 = parser.parse(expected3);
    JsonElement o6 = parser.parse(rjsd.deserialize(text3).toString());

    assertEquals(o1,o2);
    assertEquals(o3,o4);
    assertEquals(o5,o6);
  }
}

