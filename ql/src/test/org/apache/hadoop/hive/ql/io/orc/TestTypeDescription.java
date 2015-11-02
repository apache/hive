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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.orc;

import static org.junit.Assert.assertEquals;

import org.apache.orc.TypeDescription;
import org.junit.Test;

public class TestTypeDescription {

  @Test
  public void testJson() {
    TypeDescription bin = TypeDescription.createBinary();
    assertEquals("{\"category\": \"binary\", \"id\": 0, \"max\": 0}",
        bin.toJson());
    assertEquals("binary", bin.toString());
    TypeDescription struct = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal());
    assertEquals("struct<f1:int,f2:string,f3:decimal(38,10)>",
        struct.toString());
    assertEquals("{\"category\": \"struct\", \"id\": 0, \"max\": 3, \"fields\": [\n"
            + "  \"f1\": {\"category\": \"int\", \"id\": 1, \"max\": 1},\n"
            + "  \"f2\": {\"category\": \"string\", \"id\": 2, \"max\": 2},\n"
            + "  \"f3\": {\"category\": \"decimal\", \"id\": 3, \"max\": 3, \"precision\": 38, \"scale\": 10}]}",
        struct.toJson());
    struct = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createUnion()
            .addUnionChild(TypeDescription.createByte())
            .addUnionChild(TypeDescription.createDecimal()
                .withPrecision(20).withScale(10)))
        .addField("f2", TypeDescription.createStruct()
            .addField("f3", TypeDescription.createDate())
            .addField("f4", TypeDescription.createDouble())
            .addField("f5", TypeDescription.createBoolean()))
        .addField("f6", TypeDescription.createChar().withMaxLength(100));
    assertEquals("struct<f1:union<tinyint,decimal(20,10)>,f2:struct<f3:date,f4:double,f5:boolean>,f6:char(100)>",
        struct.toString());
    assertEquals(
        "{\"category\": \"struct\", \"id\": 0, \"max\": 8, \"fields\": [\n" +
            "  \"f1\": {\"category\": \"union\", \"id\": 1, \"max\": 3, \"children\": [\n" +
            "    {\"category\": \"tinyint\", \"id\": 2, \"max\": 2},\n" +
            "    {\"category\": \"decimal\", \"id\": 3, \"max\": 3, \"precision\": 20, \"scale\": 10}]},\n" +
            "  \"f2\": {\"category\": \"struct\", \"id\": 4, \"max\": 7, \"fields\": [\n" +
            "    \"f3\": {\"category\": \"date\", \"id\": 5, \"max\": 5},\n" +
            "    \"f4\": {\"category\": \"double\", \"id\": 6, \"max\": 6},\n" +
            "    \"f5\": {\"category\": \"boolean\", \"id\": 7, \"max\": 7}]},\n" +
            "  \"f6\": {\"category\": \"char\", \"id\": 8, \"max\": 8, \"length\": 100}]}",
        struct.toJson());
  }
}
