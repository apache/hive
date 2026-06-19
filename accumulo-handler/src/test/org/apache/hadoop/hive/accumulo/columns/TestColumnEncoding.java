/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.accumulo.columns;

import java.util.Map.Entry;

import org.apache.hadoop.hive.accumulo.AccumuloHiveConstants;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;

/**
 *
 */
public class TestColumnEncoding {

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidCodeThrowsException() {
    ColumnEncoding.fromCode("foo");
  }

  @Test
  public void testStringEncoding() {
    Assert.assertEquals(ColumnEncoding.STRING, ColumnEncoding.fromCode("s"));
  }

  @Test
  public void testBinaryEncoding() {
    Assert.assertEquals(ColumnEncoding.BINARY, ColumnEncoding.fromCode("b"));
  }

  @Test
  public void testMissingColumnEncoding() {
    Assert.assertFalse(ColumnEncoding.hasColumnEncoding("foo:bar"));
  }

  @Test
  public void testColumnEncodingSpecified() {
    Assert.assertTrue(ColumnEncoding.hasColumnEncoding("foo:bar#s"));
  }

  @Test
  public void testEscapedPoundIsNoEncodingSpecified() {
    Assert.assertFalse(ColumnEncoding.hasColumnEncoding("foo:b\\#ar"));
  }

  @Test
  public void testEscapedPoundWithRealPound() {
    Assert.assertTrue(ColumnEncoding.hasColumnEncoding("foo:b\\#ar#b"));
  }

  @Test
  public void testParse() {
    Assert.assertEquals(ColumnEncoding.STRING, ColumnEncoding.getFromMapping("foo:bar#s"));
  }

  @Test
  public void testParseWithEscapedPound() {
    Assert.assertEquals(ColumnEncoding.BINARY, ColumnEncoding.getFromMapping("fo\\#o:bar#b"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingEncodingOnParse() {
    ColumnEncoding.getFromMapping("foo:bar");
  }

  @Test
  public void testStripCode() {
    String mapping = "foo:bar";
    Assert.assertEquals(
        mapping,
        ColumnEncoding.stripCode(mapping + AccumuloHiveConstants.POUND
            + ColumnEncoding.BINARY.getCode()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testStripNonExistentCodeFails() {
    ColumnEncoding.stripCode("foo:bar");
  }

  @Test
  public void testStripCodeWithEscapedPound() {
    String mapping = "foo:ba\\#r";

    Assert.assertEquals(
        mapping,
        ColumnEncoding.stripCode(mapping + AccumuloHiveConstants.POUND
            + ColumnEncoding.BINARY.getCode()));
  }

  @Test
  public void testMapEncoding() {
    Assert.assertFalse(ColumnEncoding.isMapEncoding("s"));
    Assert.assertFalse(ColumnEncoding.isMapEncoding("string"));
    Assert.assertFalse(ColumnEncoding.isMapEncoding("binary"));

    Assert.assertTrue(ColumnEncoding.isMapEncoding("s:s"));
    Assert.assertTrue(ColumnEncoding.isMapEncoding("s:string"));
    Assert.assertTrue(ColumnEncoding.isMapEncoding("string:s"));
    Assert.assertTrue(ColumnEncoding.isMapEncoding("string:string"));
  }

  @Test
  public void testMapEncodingParsing() {
    Entry<ColumnEncoding,ColumnEncoding> stringString = Maps.immutableEntry(ColumnEncoding.STRING,
        ColumnEncoding.STRING), stringBinary = Maps.immutableEntry(ColumnEncoding.STRING,
        ColumnEncoding.BINARY), binaryBinary = Maps.immutableEntry(ColumnEncoding.BINARY,
        ColumnEncoding.BINARY), binaryString = Maps.immutableEntry(ColumnEncoding.BINARY,
        ColumnEncoding.STRING);

    Assert.assertEquals(stringString, ColumnEncoding.getMapEncoding("s:s"));
    Assert.assertEquals(stringString, ColumnEncoding.getMapEncoding("s:string"));
    Assert.assertEquals(stringString, ColumnEncoding.getMapEncoding("string:s"));
    Assert.assertEquals(stringString, ColumnEncoding.getMapEncoding("string:string"));

    Assert.assertEquals(stringBinary, ColumnEncoding.getMapEncoding("s:b"));
    Assert.assertEquals(stringBinary, ColumnEncoding.getMapEncoding("string:b"));
    Assert.assertEquals(stringBinary, ColumnEncoding.getMapEncoding("s:binary"));
    Assert.assertEquals(stringBinary, ColumnEncoding.getMapEncoding("string:binary"));

    Assert.assertEquals(binaryString, ColumnEncoding.getMapEncoding("b:s"));
    Assert.assertEquals(binaryString, ColumnEncoding.getMapEncoding("b:string"));
    Assert.assertEquals(binaryString, ColumnEncoding.getMapEncoding("binary:s"));
    Assert.assertEquals(binaryString, ColumnEncoding.getMapEncoding("binary:string"));

    Assert.assertEquals(binaryBinary, ColumnEncoding.getMapEncoding("b:b"));
    Assert.assertEquals(binaryBinary, ColumnEncoding.getMapEncoding("binary:b"));
    Assert.assertEquals(binaryBinary, ColumnEncoding.getMapEncoding("b:binary"));
    Assert.assertEquals(binaryBinary, ColumnEncoding.getMapEncoding("binary:binary"));
  }
}
