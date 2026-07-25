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

package org.apache.hadoop.hive.serde2.typeinfo;



import java.util.Arrays;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * TypeInfoUtils Test.
 */
public class TestTypeInfoUtils {

  static void parseTypeString(String typeString, boolean exceptionExpected) {
    boolean caughtException = false;
    try {
      TypeInfoUtils.getTypeInfoFromTypeString(typeString);
    } catch (IllegalArgumentException err) {
      caughtException = true;
    }
    assertEquals("parsing typestring " + typeString, exceptionExpected, caughtException);
  }

  @Test
  public void testTypeInfoParser() {
    String[] validTypeStrings = {
        "int",
        "string",
        "varchar(10)",
        "char(15)",
        "array<int>",
        "decimal(10,2)",
        "decimal(10, 2)",
        "decimal(10, 2 )",
        "decimal( 10, 2 )",
        "struct<user id:int,user group: int>",
        // struct field names with special characters (Iceberg partition column names)
        "struct<gpa_!@#$%^&*():double>",
        "struct<!@#$%^&*()_age:int>",
        "struct<name:string,!@#$%^&*()_age:int,gpa_!@#$%^&*():double>",
        "struct<name:string,gpa_!@#$%^&*():double>",
        "struct<gpa_!@#$%^&*():double,age:int>"
    };

    String[] invalidTypeStrings = {
        "array<",
        "varchar(123",
        "varchar(123,",
        "varchar()",
        "varchar(",
        "char(123",
        "char(123,)",
        "char()",
        "char(",
        "decimal()"
    };

    for (String typeString : validTypeStrings) {
      parseTypeString(typeString, false);
    }
    for (String typeString : invalidTypeStrings) {
      parseTypeString(typeString, true);
    }
  }

  @Test
  public void testQualifiedTypeNoParams() {
    boolean caughtException = false;
    try {
      TypeInfoUtils.getTypeInfoFromTypeString("varchar");
    } catch (Exception err) {
      caughtException = true;
    }
    assertEquals("varchar TypeInfo with no params should fail", true, caughtException);

    try {
      TypeInfoUtils.getTypeInfoFromTypeString("char");
    } catch (Exception err) {
      caughtException = true;
    }
    assertEquals("char TypeInfo with no params should fail", true, caughtException);
  }

  public static class DecimalTestCase {
    String typeString;
    int expectedPrecision;
    int expectedScale;

    public DecimalTestCase(String typeString, int expectedPrecision, int expectedScale) {
      this.typeString = typeString;
      this.expectedPrecision = expectedPrecision;
      this.expectedScale = expectedScale;
    }
  }

  @Test
  public void testDecimal() {
    DecimalTestCase[] testCases = {
        new DecimalTestCase("decimal", 10, 0),
        new DecimalTestCase("decimal(1)", 1, 0),
        new DecimalTestCase("decimal(25)", 25, 0),
        new DecimalTestCase("decimal(2,0)", 2, 0),
        new DecimalTestCase("decimal(2,1)", 2, 1),
        new DecimalTestCase("decimal(25,10)", 25, 10),
        new DecimalTestCase("decimal(38,20)", 38, 20)
    };

    for (DecimalTestCase testCase : testCases) {
      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(testCase.typeString);
      DecimalTypeInfo decimalType = (DecimalTypeInfo) typeInfo;
      assertEquals("Failed for " + testCase.typeString, testCase.expectedPrecision, decimalType.getPrecision());
      assertEquals("Failed for " + testCase.typeString, testCase.expectedScale, decimalType.getScale());
    }
  }

  @Test
  public void testStructFieldNameWithSpecialChars() {
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString("struct<gpa_!@#$%^&*():double>");
    StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
    assertEquals(1, structTypeInfo.getAllStructFieldNames().size());
    assertEquals("gpa_!@#$%^&*()", structTypeInfo.getAllStructFieldNames().get(0));
    assertEquals(TypeInfoFactory.doubleTypeInfo, structTypeInfo.getAllStructFieldTypeInfos().get(0));
  }

  @Test
  public void testStructFieldNameStartingWithSpecialChar() {
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString("struct<!@#$%^&*()_age:int>");
    StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
    assertEquals(Arrays.asList("!@#$%^&*()_age"), structTypeInfo.getAllStructFieldNames());
  }

  @Test
  public void testStructMultipleFieldsWithSpecialChars() {
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(
        "struct<name:string,!@#$%^&*()_age:int,gpa_!@#$%^&*():double>");
    StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
    assertEquals(Arrays.asList("name", "!@#$%^&*()_age", "gpa_!@#$%^&*()"),
        structTypeInfo.getAllStructFieldNames());
  }

  @Test
  public void testEmptyStruct() {
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString("struct<>");
    StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
    assertEquals(0, structTypeInfo.getAllStructFieldNames().size());
  }

  @Test
  public void testStructTrailingComma() {
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString("struct<a:int,>");
    StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
    assertEquals(Arrays.asList("a"), structTypeInfo.getAllStructFieldNames());
  }
}
