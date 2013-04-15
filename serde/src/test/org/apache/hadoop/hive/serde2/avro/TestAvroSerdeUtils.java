/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.serde2.avro;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Properties;

import static org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.EXCEPTION_MESSAGE;
import static org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.SCHEMA_LITERAL;
import static org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.SCHEMA_NONE;
import static org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.SCHEMA_URL;
import static org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.determineSchemaOrThrowException;
import static org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.getOtherTypeFromNullableType;
import static org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.isNullableType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class TestAvroSerdeUtils {
  private final String NULLABLE_UNION = "{\n" +
      "  \"type\": \"record\", \n" +
      "  \"name\": \"nullTest\",\n" +
      "  \"fields\" : [\n" +
      "    {\"name\":\"mayBeNull\", \"type\":[\"string\", \"null\"]}\n" +
      "  ]\n" +
      "}";
  // Same union, order reveresed
  private final String NULLABLE_UNION2 = "{\n" +
    "  \"type\": \"record\", \n" +
    "  \"name\": \"nullTest\",\n" +
    "  \"fields\" : [\n" +
    "    {\"name\":\"mayBeNull\", \"type\":[\"null\", \"string\"]}\n" +
    "  ]\n" +
    "}";

  private void testField(String schemaString, String fieldName, boolean shouldBeNullable) {
    Schema s = Schema.parse(schemaString);
    assertEquals(shouldBeNullable, isNullableType(s.getField(fieldName).schema()));
  }

  @Test
  public void isNullableTypeAcceptsNullableUnions() {
    testField(NULLABLE_UNION, "mayBeNull", true);
    testField(NULLABLE_UNION2, "mayBeNull", true);
  }

  @Test
  public void isNullableTypeIdentifiesUnionsOfMoreThanTwoTypes() {
    String schemaString = "{\n" +
      "  \"type\": \"record\", \n" +
      "  \"name\": \"shouldNotPass\",\n" +
      "  \"fields\" : [\n" +
      "    {\"name\":\"mayBeNull\", \"type\":[\"string\", \"int\", \"null\"]}\n" +
      "  ]\n" +
      "}";
    testField(schemaString, "mayBeNull", false);
  }

  @Test
  public void isNullableTypeIdentifiesUnionsWithoutNulls() {
    String s = "{\n" +
      "  \"type\": \"record\", \n" +
      "  \"name\": \"unionButNoNull\",\n" +
      "  \"fields\" : [\n" +
      "    {\"name\":\"a\", \"type\":[\"int\", \"string\"]}\n" +
      "  ]\n" +
      "}";
    testField(s, "a", false);
  }

  @Test
  public void isNullableTypeIdentifiesNonUnionTypes() {
    String schemaString = "{\n" +
      "  \"type\": \"record\", \n" +
      "  \"name\": \"nullTest2\",\n" +
      "  \"fields\" : [\n" +
      "    {\"name\":\"justAnInt\", \"type\":\"int\"}\n" +
      "  ]\n" +
      "}";
    testField(schemaString, "justAnInt", false);
  }

  @Test
  public void getTypeFromNullableTypePositiveCase() {
    Schema s = Schema.parse(NULLABLE_UNION);
    Schema typeFromNullableType = getOtherTypeFromNullableType(s.getField("mayBeNull").schema());
    assertEquals(Schema.Type.STRING, typeFromNullableType.getType());

    s = Schema.parse(NULLABLE_UNION2);
    typeFromNullableType = getOtherTypeFromNullableType(s.getField("mayBeNull").schema());
    assertEquals(Schema.Type.STRING, typeFromNullableType.getType());
  }

  @Test(expected=AvroSerdeException.class)
  public void determineSchemaThrowsExceptionIfNoSchema() throws IOException, AvroSerdeException {
    Properties prop = new Properties();
    AvroSerdeUtils.determineSchemaOrThrowException(prop);
  }

  @Test
  public void determineSchemaFindsLiterals() throws Exception {
    String schema = TestAvroObjectInspectorGenerator.RECORD_SCHEMA;
    Properties props = new Properties();
    props.put(AvroSerdeUtils.SCHEMA_LITERAL, schema);
    Schema expected = Schema.parse(schema);
    assertEquals(expected, AvroSerdeUtils.determineSchemaOrThrowException(props));
  }

  @Test
  public void detemineSchemaTriesToOpenUrl() throws AvroSerdeException, IOException {
    Properties props = new Properties();
    props.put(AvroSerdeUtils.SCHEMA_URL, "not:///a.real.url");

    try {
      AvroSerdeUtils.determineSchemaOrThrowException(props);
      fail("Should have tried to open that URL");
    } catch(MalformedURLException e) {
      assertEquals("unknown protocol: not", e.getMessage());
    }
  }

  @Test
  public void noneOptionWorksForSpecifyingSchemas() throws IOException, AvroSerdeException {
    Properties props = new Properties();

    // Combo 1: Both set to none
    props.put(SCHEMA_URL, SCHEMA_NONE);
    props.put(SCHEMA_LITERAL, SCHEMA_NONE);
    try {
      determineSchemaOrThrowException(props);
      fail("Should have thrown exception with none set for both url and literal");
    } catch(AvroSerdeException he) {
      assertEquals(EXCEPTION_MESSAGE, he.getMessage());
    }

    // Combo 2: Literal set, url set to none
    props.put(SCHEMA_LITERAL, TestAvroObjectInspectorGenerator.RECORD_SCHEMA);
    Schema s;
    try {
      s = determineSchemaOrThrowException(props);
      assertNotNull(s);
      assertEquals(Schema.parse(TestAvroObjectInspectorGenerator.RECORD_SCHEMA), s);
    } catch(AvroSerdeException he) {
      fail("Should have parsed schema literal, not thrown exception.");
    }

    // Combo 3: url set, literal set to none
    props.put(SCHEMA_LITERAL, SCHEMA_NONE);
    props.put(SCHEMA_URL, "not:///a.real.url");
    try {
      determineSchemaOrThrowException(props);
      fail("Should have tried to open that bogus URL");
    } catch(MalformedURLException e) {
      assertEquals("unknown protocol: not", e.getMessage());
    }
  }

  @Test
  public void determineSchemaCanReadSchemaFromHDFS() throws IOException, AvroSerdeException {
    String schemaString = TestAvroObjectInspectorGenerator.RECORD_SCHEMA;
    MiniDFSCluster miniDfs = null;
    try {
      // MiniDFSCluster litters files and folders all over the place.
      miniDfs = new MiniDFSCluster(new Configuration(), 1, true, null);

      miniDfs.getFileSystem().mkdirs(new Path("/path/to/schema"));
      FSDataOutputStream out = miniDfs.getFileSystem()
              .create(new Path("/path/to/schema/schema.avsc"));
      out.writeBytes(schemaString);
      out.close();
      String onHDFS = miniDfs.getFileSystem().getUri() + "/path/to/schema/schema.avsc";

      Schema schemaFromHDFS =
              AvroSerdeUtils.getSchemaFromHDFS(onHDFS, miniDfs.getFileSystem().getConf());
      Schema expectedSchema = Schema.parse(schemaString);
      assertEquals(expectedSchema, schemaFromHDFS);
    } finally {
      if(miniDfs != null) miniDfs.shutdown();
    }
  }
}
