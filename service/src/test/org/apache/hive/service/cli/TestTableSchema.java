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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.service.cli;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.thrift.Type;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class TestTableSchema {

  private static final TableSchema SCHEMA = new TableSchema(List.of(
      new FieldSchema("id", "int", null),
      new FieldSchema("v", "variant", "payload"),
      new FieldSchema("vs", "array<variant>", null)));

  @Test
  public void testTopLevelVariantIsReportedAsString() {
    ColumnDescriptor v = SCHEMA.withVariantAsString().getColumnDescriptorAt(1);

    assertEquals(Type.STRING_TYPE, v.getType());
    assertEquals("v", v.getName());
    assertEquals("payload", v.getComment());
    assertEquals(2, v.getOrdinalPosition());
  }

  @Test
  public void testOtherColumnsAreKept() {
    TableSchema downgraded = SCHEMA.withVariantAsString();

    assertSame(SCHEMA.getColumnDescriptorAt(0), downgraded.getColumnDescriptorAt(0));
    assertSame(SCHEMA.getColumnDescriptorAt(2), downgraded.getColumnDescriptorAt(2));
    assertEquals(Type.ARRAY_TYPE, downgraded.getColumnDescriptorAt(2).getType());
    assertEquals(3, downgraded.getSize());
  }

  @Test
  public void testSourceSchemaIsUnchanged() {
    SCHEMA.withVariantAsString();

    assertEquals(Type.VARIANT_TYPE, SCHEMA.getColumnDescriptorAt(1).getType());
  }
}
