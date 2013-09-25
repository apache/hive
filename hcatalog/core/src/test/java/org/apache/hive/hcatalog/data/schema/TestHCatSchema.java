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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.data.schema;

import junit.framework.TestCase;
import org.apache.hive.hcatalog.common.HCatException;

import java.util.ArrayList;
import java.util.List;

public class TestHCatSchema extends TestCase {
  public void testCannotAddFieldMoreThanOnce() throws HCatException {
    List<HCatFieldSchema> fieldSchemaList = new ArrayList<HCatFieldSchema>();
    fieldSchemaList.add(new HCatFieldSchema("name", HCatFieldSchema.Type.STRING, "What's your handle?"));
    fieldSchemaList.add(new HCatFieldSchema("age", HCatFieldSchema.Type.INT, "So very old"));

    HCatSchema schema = new HCatSchema(fieldSchemaList);

    assertTrue(schema.getFieldNames().contains("age"));
    assertEquals(2, schema.getFields().size());

    try {
      schema.append(new HCatFieldSchema("age", HCatFieldSchema.Type.INT, "So very old"));
      fail("Was able to append field schema with same name");
    } catch (HCatException he) {
      assertTrue(he.getMessage().contains("Attempt to append HCatFieldSchema with already existing name: age."));
    }

    assertTrue(schema.getFieldNames().contains("age"));
    assertEquals(2, schema.getFields().size());

    // Should also not be able to add fields of different types with same name
    try {
      schema.append(new HCatFieldSchema("age", HCatFieldSchema.Type.STRING, "Maybe spelled out?"));
      fail("Was able to append field schema with same name");
    } catch (HCatException he) {
      assertTrue(he.getMessage().contains("Attempt to append HCatFieldSchema with already existing name: age."));
    }

    assertTrue(schema.getFieldNames().contains("age"));
    assertEquals(2, schema.getFields().size());
  }

  public void testHashCodeEquals() throws HCatException {
    HCatFieldSchema memberID1 = new HCatFieldSchema("memberID", HCatFieldSchema.Type.INT, "as a number");
    HCatFieldSchema memberID2 = new HCatFieldSchema("memberID", HCatFieldSchema.Type.INT, "as a number");
    assertTrue("Expected objects to be equal", memberID1.equals(memberID2));
    assertTrue("Expected hash codes to be equal", memberID1.hashCode() == memberID2.hashCode());
  }

  public void testCannotInstantiateSchemaWithRepeatedFieldNames() throws HCatException {
    List<HCatFieldSchema> fieldSchemaList = new ArrayList<HCatFieldSchema>();

    fieldSchemaList.add(new HCatFieldSchema("memberID", HCatFieldSchema.Type.INT, "as a number"));
    fieldSchemaList.add(new HCatFieldSchema("location", HCatFieldSchema.Type.STRING, "there's Waldo"));

    // No duplicate names.  This should be ok
    HCatSchema schema = new HCatSchema(fieldSchemaList);

    fieldSchemaList.add(new HCatFieldSchema("memberID", HCatFieldSchema.Type.STRING, "as a String"));

    // Now a duplicated field name.  Should fail
    try {
      HCatSchema schema2 = new HCatSchema(fieldSchemaList);
      fail("Able to add duplicate field name");
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("Field named memberID already exists"));
    }
  }
  public void testRemoveAddField() throws HCatException {
    List<HCatFieldSchema> fieldSchemaList = new ArrayList<HCatFieldSchema>();

    fieldSchemaList.add(new HCatFieldSchema("memberID", HCatFieldSchema.Type.INT, "as a number"));
    HCatFieldSchema locationField = new HCatFieldSchema("location", HCatFieldSchema.Type.STRING, "there's Waldo");
    fieldSchemaList.add(locationField);
    HCatSchema schema = new HCatSchema(fieldSchemaList);
    schema.remove(locationField);
    Integer position = schema.getPosition(locationField.getName());
    assertTrue("position is not null after remove" , position == null);
    try {
      schema.append(locationField);
    }
    catch (HCatException ex) {
      assertFalse(ex.getMessage(), true);
    }
  }
}
