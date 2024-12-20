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
package org.apache.hadoop.hive.metastore.properties;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

import static org.apache.hadoop.hive.metastore.properties.PropertyType.DATETIME;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.DOUBLE;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.INTEGER;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.JSON;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.STRING;

public class PropertySchemaTest {

  @Test public void testSchema0() throws IOException {
    PropertySchema schema = new PropertySchema("table");
    Assert.assertTrue(schema.declareProperty("id", INTEGER));
    Assert.assertTrue(schema.declareProperty("uuid", STRING));
    Assert.assertTrue(schema.declareProperty("creation_date", DATETIME));
    Assert.assertTrue(schema.declareProperty("policy", JSON));
    UUID uuidnofill = schema.getDigest();
    Assert.assertTrue(schema.declareProperty("fill_ratio", DOUBLE, 0.75d));
    // redefinition with different type fails with error
    try {
      schema.declareProperty("id", DOUBLE);
      Assert.fail("should have failed, id is already declared with a different type");
    } catch(IllegalArgumentException xill) {
      Assert.assertTrue(xill.getMessage().contains("id"));
    }
    // redefinition with same type just does nothing
    Assert.assertFalse(schema.declareProperty("id", INTEGER));

    byte[] b0 = SerializationProxy.toBytes(schema);
    PropertySchema s1 = SerializationProxy.fromBytes(b0);
    Assert.assertTrue(schema != s1);
    Assert.assertEquals(schema, s1);

    UUID uuid0 = schema.getDigest();
    Assert.assertTrue(schema.removeProperty("fill_ratio"));
    Assert.assertFalse(schema.removeProperty("fill_ratio"));
    UUID uuid1 = schema.getDigest();
    Assert.assertNotEquals(uuid0, uuid1);
    Assert.assertEquals(uuidnofill, uuid1);
  }
}
