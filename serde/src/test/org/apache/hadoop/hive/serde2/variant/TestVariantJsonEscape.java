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

package org.apache.hadoop.hive.serde2.variant;

import java.io.CharArrayWriter;
import java.io.IOException;
import java.time.ZoneOffset;
import java.util.List;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestVariantJsonEscape {

  // control characters, quote, backslash, slash, DEL, non-ASCII, a surrogate pair, U+2028/2029, an empty string
  private static final List<String> STRINGS = List.of("", "plain", "a\"b\\c/d", "\u0000\u0001\b\t\n\u000b\f\r\u001f",
      "\u007f", "é ü ß 中文", "😀 emoji", "line para end", "￿￾",
      "\"" + "x\\\n/".repeat(1000) + "\"");

  /** The JSON string a Jackson generator writes for {@code str}. */
  private static String generated(String str) throws IOException {
    try (CharArrayWriter writer = new CharArrayWriter();
         JsonGenerator gen = new JsonFactory().createGenerator(writer)) {
      gen.writeString(str);
      gen.flush();
      return writer.toString();
    }
  }

  private static String escaped(String str) {
    StringBuilder sb = new StringBuilder();
    Variant.escapeJson(sb, str);
    return sb.toString();
  }

  @Test
  public void testEveryCharacterIsEscapedAsTheGeneratorDoes() throws IOException {
    // every UTF-16 code unit on its own, lone surrogates included
    for (char c = 0; c < Character.MAX_VALUE; c++) {
      String str = String.valueOf(c);
      assertEquals(Integer.toHexString(c), generated(str), escaped(str));
    }
    for (String str : STRINGS) {
      assertEquals(str, generated(str), escaped(str));
    }
    assertEquals("\"\\u0000\\n\\\"\\\\/ \"", escaped("\u0000\n\"\\/ "));
  }

  @Test
  public void testToJsonEscapesKeysAndStrings() throws IOException {
    for (String str : STRINGS) {
      String json = generated(str);
      Variant string = VariantBuilder.parseJson(json, false);
      assertEquals(json, string.toJson(ZoneOffset.UTC));
      Variant object = VariantBuilder.parseJson("{" + json + ":[" + json + "]}", false);
      assertEquals("{" + json + ":[" + json + "]}", object.toJson(ZoneOffset.UTC));
    }
  }
}
