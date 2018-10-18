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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class TestStringExpr {
  @Test
  public void test() throws Exception {
    StringExpr.Finder pattern = compile("pattern");
    assertNotNull(pattern);

    StringExpr.Finder patternOneChar = compile("g");
    assertNotNull(patternOneChar);

    StringExpr.Finder patternZero = compile("");
    assertNotNull(patternZero);

    String input1 = "string that contains a patterN...";
    String input2 = "string that contains a pattern...";
    String input3 = "pattern at the start of a string";
    String input4 = "string that ends with a pattern";

    assertEquals("Testing invalid match", -1, find(pattern, input1));
    assertEquals("Testing valid match", 23, find(pattern, input2));
    assertEquals("Testing single-character match", 5, find(patternOneChar, input1));
    assertEquals("Testing zero-length pattern", 0, find(patternZero, input1));
    assertEquals("Testing match at start of string", 0, find(pattern, input3));
    assertEquals("Testing match at end of string", 24, find(pattern, input4));
  }

  private StringExpr.Finder compile(String pattern) {
    return StringExpr.compile(pattern.getBytes(StandardCharsets.UTF_8));
  }

  private int find(StringExpr.Finder finder, String string) {
    byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
    return finder.find(bytes, 0, bytes.length);
  }
}