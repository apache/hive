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
package org.apache.orc.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.orc.TypeDescription;
import org.junit.Test;

public class TestSchemaEvolution {

  @Test
  public void testDataTypeConversion1() throws IOException {
    TypeDescription fileStruct1 = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(38).withScale(10));
    SchemaEvolution same1 = new SchemaEvolution(fileStruct1, null);
    assertFalse(same1.hasConversion());
    TypeDescription readerStruct1 = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(38).withScale(10));
    SchemaEvolution both1 = new SchemaEvolution(fileStruct1, readerStruct1, null);
    assertFalse(both1.hasConversion());
    TypeDescription readerStruct1diff = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createLong())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(38).withScale(10));
    SchemaEvolution both1diff = new SchemaEvolution(fileStruct1, readerStruct1diff, null);
    assertTrue(both1diff.hasConversion());
    TypeDescription readerStruct1diffPrecision = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(12).withScale(10));
    SchemaEvolution both1diffPrecision = new SchemaEvolution(fileStruct1, readerStruct1diffPrecision, null);
    assertTrue(both1diffPrecision.hasConversion());
  }

  @Test
  public void testDataTypeConversion2() throws IOException {
    TypeDescription fileStruct2 = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createUnion()
            .addUnionChild(TypeDescription.createByte())
            .addUnionChild(TypeDescription.createDecimal()
                .withPrecision(20).withScale(10)))
        .addField("f2", TypeDescription.createStruct()
            .addField("f3", TypeDescription.createDate())
            .addField("f4", TypeDescription.createDouble())
            .addField("f5", TypeDescription.createBoolean()))
        .addField("f6", TypeDescription.createChar().withMaxLength(100));
    SchemaEvolution same2 = new SchemaEvolution(fileStruct2, null);
    assertFalse(same2.hasConversion());
    TypeDescription readerStruct2 = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createUnion()
            .addUnionChild(TypeDescription.createByte())
            .addUnionChild(TypeDescription.createDecimal()
                .withPrecision(20).withScale(10)))
        .addField("f2", TypeDescription.createStruct()
            .addField("f3", TypeDescription.createDate())
            .addField("f4", TypeDescription.createDouble())
            .addField("f5", TypeDescription.createBoolean()))
        .addField("f6", TypeDescription.createChar().withMaxLength(100));
    SchemaEvolution both2 = new SchemaEvolution(fileStruct2, readerStruct2, null);
    assertFalse(both2.hasConversion());
    TypeDescription readerStruct2diff = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createUnion()
            .addUnionChild(TypeDescription.createByte())
            .addUnionChild(TypeDescription.createDecimal()
                .withPrecision(20).withScale(10)))
        .addField("f2", TypeDescription.createStruct()
            .addField("f3", TypeDescription.createDate())
            .addField("f4", TypeDescription.createDouble()))
        .addField("f6", TypeDescription.createChar().withMaxLength(100));
    SchemaEvolution both2diff = new SchemaEvolution(fileStruct2, readerStruct2diff, null);
    assertTrue(both2diff.hasConversion());
    TypeDescription readerStruct2diffChar = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createUnion()
            .addUnionChild(TypeDescription.createByte())
            .addUnionChild(TypeDescription.createDecimal()
                .withPrecision(20).withScale(10)))
        .addField("f2", TypeDescription.createStruct()
            .addField("f3", TypeDescription.createDate())
            .addField("f4", TypeDescription.createDouble()))
        .addField("f6", TypeDescription.createChar().withMaxLength(80));
    SchemaEvolution both2diffChar = new SchemaEvolution(fileStruct2, readerStruct2diffChar, null);
    assertTrue(both2diffChar.hasConversion());
  }
}
