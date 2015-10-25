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
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

//import static org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.AVRO_SERDE_SCHEMA;
//import static org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.SCHEMA_LITERAL;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.AvroTableProperties;
import static org.junit.Assert.assertEquals;
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

  static final Schema originalSchema = AvroSerdeUtils.getSchemaFor(originalSchemaString);
  static final Schema newSchema = AvroSerdeUtils.getSchemaFor(newSchemaString);

  @Test
  public void initializeDoesNotReuseSchemasFromConf() throws SerDeException {
    // Hive will re-use the Configuration object that it passes in to be
    // initialized.  Therefore we need to make sure we don't look for any
    // old schemas within it.
    Configuration conf = new Configuration();
    conf.set(AvroTableProperties.AVRO_SERDE_SCHEMA.getPropName(), originalSchema.toString(false));

    Properties props = new Properties();
    props.put(AvroTableProperties.SCHEMA_LITERAL.getPropName(), newSchemaString);


    AvroSerDe asd = new AvroSerDe();
    SerDeUtils.initializeSerDe(asd, conf, props, null);

    // Verify that the schema now within the configuration is the one passed
    // in via the properties
    assertEquals(newSchema, AvroSerdeUtils.getSchemaFor(conf.get(AvroTableProperties.AVRO_SERDE_SCHEMA.getPropName())));
  }

  @Test
  public void noSchemaProvidedThrowsException() {
    Properties props = new Properties();

    verifyExpectedException(props);
  }

  @Test
  public void gibberishSchemaProvidedReturnsErrorSchema() {
    Properties props = new Properties();
    props.put(AvroTableProperties.SCHEMA_LITERAL.getPropName(), "blahblahblah");

    verifyExpectedException(props);
  }

  @Test
  public void emptySchemaProvidedThrowsException() {
    Properties props = new Properties();
    props.put(AvroTableProperties.SCHEMA_LITERAL.getPropName(), "");

    verifyExpectedException(props);
  }

  @Test
  public void badSchemaURLProvidedThrowsException() {
    Properties props = new Properties();
    props.put(AvroTableProperties.SCHEMA_URL.getPropName(), "not://a/url");

    verifyExpectedException(props);
  }

  @Test
  public void emptySchemaURLProvidedThrowsException() {
    Properties props = new Properties();
    props.put(AvroTableProperties.SCHEMA_URL.getPropName(), "");

    verifyExpectedException(props);
  }

  @Test
  public void bothPropertiesSetToNoneThrowsException() {
    Properties props = new Properties();
    props.put(AvroTableProperties.SCHEMA_URL.getPropName(), AvroSerdeUtils.SCHEMA_NONE);
    props.put(AvroTableProperties.SCHEMA_LITERAL.getPropName(), AvroSerdeUtils.SCHEMA_NONE);

    verifyExpectedException(props);
  }

  private void verifyExpectedException(Properties props) {
    AvroSerDe asd = new AvroSerDe();
    try {
      SerDeUtils.initializeSerDe(asd, new Configuration(), props, null);
      fail("Expected Exception did not be thrown");
    } catch (SerDeException e) {
      // good
    }
  }

  @Test
  public void getSerializedClassReturnsCorrectType() {
    AvroSerDe asd = new AvroSerDe();
    assertEquals(AvroGenericRecordWritable.class, asd.getSerializedClass());
  }
}
