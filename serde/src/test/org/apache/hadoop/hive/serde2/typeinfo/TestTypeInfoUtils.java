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

import junit.framework.TestCase;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class TestTypeInfoUtils extends TestCase {

  static void parseTypeString(String typeString, boolean exceptionExpected) {
    boolean caughtException = false;
    try {
      TypeInfoUtils.getTypeInfoFromTypeString(typeString);
    } catch (IllegalArgumentException err) {
      caughtException = true;
    }
    assertEquals("parsing typestring " + typeString, exceptionExpected, caughtException);
  }

  public void testTypeInfoParser() {
    String[] validTypeStrings = {
        "int",
        "string",
        "varchar(10)",
        "char(15)",
        "array<int>"
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
}
