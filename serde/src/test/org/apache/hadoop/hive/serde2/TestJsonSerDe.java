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
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test suite for the JSON SerDe class.
 */
public class TestJsonSerDe {

  @Test
  public void testPrimativeDataTypes() throws Exception {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS,
        "name,height,weight,endangered,born");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES,
        "string,float,int,boolean,timestamp");
    props.setProperty(serdeConstants.TIMESTAMP_FORMATS, "millis");

    JsonSerDe serde = new JsonSerDe();
    serde.initialize(null, props, false);

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
  public void testArray() throws Exception {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, "list,items");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, "string,array<string>");
    props.setProperty(serdeConstants.TIMESTAMP_FORMATS, "millis");

    JsonSerDe serde = new JsonSerDe();
    serde.initialize(null, props, false);

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
   * Unquoted strings (hence an int value) is allowed by Javascript, but not by
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
    serde.initialize(null, props, false);

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
    serde.initialize(null, props, false);

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
    serde.initialize(null, props, false);

    serde.deserialize(new Text(""));
  }

  @Test
  public void testChar() throws Exception {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, "a");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, "char(5)");
    props.setProperty(serdeConstants.TIMESTAMP_FORMATS, "millis");

    JsonSerDe serde = new JsonSerDe();
    serde.initialize(null, props, true);

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
    serde.initialize(null, props, false);

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
    serde.initialize(null, props, false);

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
    serde.initialize(null, props, false);

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
        "yyyy-MM-dd'T'HH:mm:ss'Z");

    final TimeZone localTz = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("US/Pacific"));

    try {
      JsonSerDe serde = new JsonSerDe();
      serde.initialize(null, props, false);

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
}
