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

import java.util.List;
import org.apache.avro.Schema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


public class TestSchemaToTypeInfo {

  @Rule
  public ExpectedException expect = ExpectedException.none();

  @Test
  public void testDisallowRecursiveSchema()
      throws AvroSerdeException {

    expect.expect(AvroSerdeException.class);
    expect.expectMessage("Recursive schemas are not supported");

    final String schemaString = "{\n"
        + "  \"type\" : \"record\",\n"
        + "  \"name\" : \"Cycle\",\n"
        + "  \"namespace\" : \"org.apache.hadoop.hive.serde2.avro\",\n"
        + "  \"fields\" : [ {\n"
        + "    \"name\" : \"child\",\n"
        + "    \"type\" : [ \"null\", \"Cycle\"],\n"
        + "    \"default\" : null\n"
        + "  } ]\n"
        + "}";

    List<TypeInfo> types = SchemaToTypeInfo.generateColumnTypes(new Schema.Parser().parse(schemaString));
  }
}