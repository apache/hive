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
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Writable;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Properties;

import static org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.AVRO_SERDE_SCHEMA;
import static org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.SCHEMA_LITERAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestAvroSerde {
     static final String originalSchemaString = "{\n" +
        "    \"namespace\": \"org.apache.hadoop.hive\",\n" +
        "    \"name\": \"previous\",\n" +
        "    \"type\": \"record\",\n" +
        "    \"fields\": [\n" +
        "        {\n" +
        "            \"name\":\"text\",\n" +
        "            \"type\":\"string\"\n" +
        "        }\n" +
        "    ]\n" +
        "}";
   static final String newSchemaString = "{\n" +
      "    \"namespace\": \"org.apache.hadoop.hive\",\n" +
      "    \"name\": \"new\",\n" +
      "    \"type\": \"record\",\n" +
      "    \"fields\": [\n" +
      "        {\n" +
      "            \"name\":\"text\",\n" +
      "            \"type\":\"string\"\n" +
      "        }\n" +
      "    ]\n" +
      "}";

  static final Schema originalSchema = Schema.parse(originalSchemaString);
  static final Schema newSchema = Schema.parse(newSchemaString);

  @Test
  public void initializeDoesNotReuseSchemasFromConf() throws SerDeException {
    // Hive will re-use the Configuration object that it passes in to be
    // initialized.  Therefore we need to make sure we don't look for any
    // old schemas within it.
    Configuration conf = new Configuration();
    conf.set(AVRO_SERDE_SCHEMA, originalSchema.toString(false));

    Properties props = new Properties();
    props.put(SCHEMA_LITERAL, newSchemaString);


    AvroSerDe asd = new AvroSerDe();
    asd.initialize(conf, props);

    // Verify that the schema now within the configuration is the one passed
    // in via the properties
    assertEquals(newSchema, Schema.parse(conf.get(AVRO_SERDE_SCHEMA)));
  }

  @Test
  public void noSchemaProvidedReturnsErrorSchema() throws SerDeException {
    Properties props = new Properties();

    verifyErrorSchemaReturned(props);
  }

  @Test
  public void gibberishSchemaProvidedReturnsErrorSchema() throws SerDeException {
    Properties props = new Properties();
    props.put(AvroSerdeUtils.SCHEMA_LITERAL, "blahblahblah");

    verifyErrorSchemaReturned(props);
  }

  @Test
  public void emptySchemaProvidedReturnsErrorSchema() throws SerDeException {
    Properties props = new Properties();
    props.put(AvroSerdeUtils.SCHEMA_LITERAL, "");

    verifyErrorSchemaReturned(props);
  }

  @Test
  public void badSchemaURLProvidedReturnsErrorSchema() throws SerDeException {
    Properties props = new Properties();
    props.put(AvroSerdeUtils.SCHEMA_URL, "not://a/url");

    verifyErrorSchemaReturned(props);
  }

  @Test
  public void emptySchemaURLProvidedReturnsErrorSchema() throws SerDeException {
    Properties props = new Properties();
    props.put(AvroSerdeUtils.SCHEMA_URL, "");

    verifyErrorSchemaReturned(props);
  }

  @Test
  public void bothPropertiesSetToNoneReturnsErrorSchema() throws SerDeException {
    Properties props = new Properties();
    props.put(AvroSerdeUtils.SCHEMA_URL, AvroSerdeUtils.SCHEMA_NONE);
    props.put(AvroSerdeUtils.SCHEMA_LITERAL, AvroSerdeUtils.SCHEMA_NONE);

    verifyErrorSchemaReturned(props);
  }

  private void verifyErrorSchemaReturned(Properties props) throws SerDeException {
    AvroSerDe asd = new AvroSerDe();
    asd.initialize(new Configuration(), props);
    assertTrue(asd.getObjectInspector() instanceof StandardStructObjectInspector);
    StandardStructObjectInspector oi = (StandardStructObjectInspector)asd.getObjectInspector();
    List<? extends StructField> allStructFieldRefs = oi.getAllStructFieldRefs();
    assertEquals(SchemaResolutionProblem.SIGNAL_BAD_SCHEMA.getFields().size(), allStructFieldRefs.size());
    StructField firstField = allStructFieldRefs.get(0);
    assertTrue(firstField.toString().contains("error_error_error_error_error_error_error"));

    try {
      Writable mock = Mockito.mock(Writable.class);
      asd.deserialize(mock);
      fail("Should have thrown a BadSchemaException");
    } catch (BadSchemaException bse) {
      // good
    }

    try {
      Object o = Mockito.mock(Object.class);
      ObjectInspector mockOI = Mockito.mock(ObjectInspector.class);
      asd.serialize(o, mockOI);
      fail("Should have thrown a BadSchemaException");
    } catch (BadSchemaException bse) {
      // good
    }
  }

  @Test
  public void getSerializedClassReturnsCorrectType() {
    AvroSerDe asd = new AvroSerDe();
    assertEquals(AvroGenericRecordWritable.class, asd.getSerializedClass());
  }
}
