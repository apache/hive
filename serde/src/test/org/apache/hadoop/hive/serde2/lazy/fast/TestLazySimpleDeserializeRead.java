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
package org.apache.hadoop.hive.serde2.lazy.fast;



import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 * Unit tests for LazySimpleDeserializeRead.
 *
 */
public class TestLazySimpleDeserializeRead {

  /**
   * Test for escaping.
   *
   */
  @Test
  public void testEscaping() throws Exception {
    HiveConf hconf = new HiveConf();

    // set the escaping related properties
    Properties props = new Properties();
    props.setProperty(serdeConstants.FIELD_DELIM, "|");
    props.setProperty(serdeConstants.ESCAPE_CHAR, "\\");
    props.setProperty(serdeConstants.SERIALIZATION_ESCAPE_CRLF, "true");

    LazySerDeParameters lazyParams =
        new LazySerDeParameters(hconf, props,
            LazySimpleSerDe.class.getName());

    TypeInfo[] typeInfos = new TypeInfo[2];
    typeInfos[0] = TypeInfoFactory.getPrimitiveTypeInfo("string");
    typeInfos[1] = TypeInfoFactory.getPrimitiveTypeInfo("string");

    LazySimpleDeserializeRead deserializeRead =
        new LazySimpleDeserializeRead(typeInfos, null, true, lazyParams);

    // set and parse the row
    String s = "This\\nis\\rthe first\\r\\nmulti-line field\\n|field1-2";
    Text row = new Text(s.getBytes("UTF-8"));
    deserializeRead.set(row.getBytes(), 0, row.getLength());

    assertTrue(deserializeRead.readNextField());
    assertTrue(deserializeRead.currentExternalBufferNeeded);

    int externalBufferLen = deserializeRead.currentExternalBufferNeededLen;
    assertEquals("Wrong external buffer length", externalBufferLen, 36);

    byte[] externalBuffer = new byte[externalBufferLen];
    deserializeRead.copyToExternalBuffer(externalBuffer, 0);

    Text field = new Text();
    field.set(externalBuffer, 0, externalBufferLen);

    String f = "This\nis\rthe first\r\nmulti-line field\n";
    Text escaped = new Text(f.getBytes("UTF-8"));

    assertTrue("The escaped result is incorrect", field.compareTo(escaped) == 0);
  }

  // --- single-byte field delimiter, no escape (LazySimple fast path) --------

  /**
   * Mixed INT / STRING columns on the single-byte hot loop — the numeric
   * parsers depend on the length arithmetic between {@code startPositions[i]}
   * and {@code startPositions[i+1]} charging exactly 1 byte for the delim.
   */
  @Test
  public void testSingleByteNoEscapeMixedTypes() throws Exception {
    LazySerDeParameters params = singleDelimParams("|");
    TypeInfo[] typeInfos = new TypeInfo[] {
        TypeInfoFactory.intTypeInfo,
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.longTypeInfo
    };
    LazySimpleDeserializeRead r = new LazySimpleDeserializeRead(typeInfos, null, true, params);

    byte[] row = "42|hello|9876543210".getBytes(StandardCharsets.UTF_8);
    r.set(row, 0, row.length);

    assertTrue(r.readNextField());
    assertEquals(42, r.currentInt);

    assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), readStringField(r));

    assertTrue(r.readNextField());
    assertEquals(9876543210L, r.currentLong);
  }

  /**
   * A row ending exactly on a delim: the last field is empty. Exercises the
   * "separator hit, then loop terminates because fieldByteEnd == end" arc in
   * the hot loop and the trailing "end serves as final separator" branch.
   */
  @Test
  public void testSingleByteNoEscapeEmptyLastField() throws Exception {
    LazySerDeParameters params = singleDelimParams("|");
    TypeInfo[] typeInfos = new TypeInfo[] {
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo
    };
    LazySimpleDeserializeRead r = new LazySimpleDeserializeRead(typeInfos, null, true, params);

    byte[] row = "one|".getBytes(StandardCharsets.UTF_8);
    r.set(row, 0, row.length);

    assertArrayEquals("one".getBytes(StandardCharsets.UTF_8), readStringField(r));
    assertTrue("empty last field should be non-null (zero-length)", r.readNextField());
    assertEquals(0, r.currentBytesLength);
  }

  /**
   * A middle field that's empty (two consecutive delims). Ensures the
   * "fieldByteBegin = ++fieldByteEnd" step correctly records a zero-length
   * span in {@code startPositions}, not a spurious NULL.
   */
  @Test
  public void testSingleByteNoEscapeEmptyMiddleField() throws Exception {
    LazySerDeParameters params = singleDelimParams("|");
    TypeInfo[] typeInfos = new TypeInfo[] {
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo
    };
    LazySimpleDeserializeRead r = new LazySimpleDeserializeRead(typeInfos, null, true, params);

    byte[] row = "a||c".getBytes(StandardCharsets.UTF_8);
    r.set(row, 0, row.length);

    assertArrayEquals("a".getBytes(StandardCharsets.UTF_8), readStringField(r));
    assertTrue("empty middle field must be non-null", r.readNextField());
    assertEquals(0, r.currentBytesLength);
    assertArrayEquals("c".getBytes(StandardCharsets.UTF_8), readStringField(r));
  }

  /**
   * Row has fewer fields than the schema expects. All missing fields must
   * come back as NULL — this is the sentinel-fill branch of
   * {@link org.apache.hadoop.hive.serde2.lazy.fast.LazySimpleDeserializeRead#topLevelParse()}.
   */
  @Test
  public void testSingleByteNoEscapeMissingTrailingFields() throws Exception {
    LazySerDeParameters params = singleDelimParams("|");
    TypeInfo[] typeInfos = new TypeInfo[] {
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo
    };
    LazySimpleDeserializeRead r = new LazySimpleDeserializeRead(typeInfos, null, true, params);

    byte[] row = "only-one".getBytes(StandardCharsets.UTF_8);
    r.set(row, 0, row.length);

    assertArrayEquals("only-one".getBytes(StandardCharsets.UTF_8), readStringField(r));
    assertFalse(r.readNextField());
    assertFalse(r.readNextField());
  }

  /**
   * Row supplies more delims than the schema has columns: the hot loop must
   * break out on {@code fieldId == fieldCount} without consuming past the
   * last expected field.
   */
  @Test
  public void testSingleByteNoEscapeExtraDelimsIgnored() throws Exception {
    LazySerDeParameters params = singleDelimParams("|");
    TypeInfo[] typeInfos = new TypeInfo[] {
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo
    };
    LazySimpleDeserializeRead r = new LazySimpleDeserializeRead(typeInfos, null, true, params);

    byte[] row = "a|b|extra|ignored".getBytes(StandardCharsets.UTF_8);
    r.set(row, 0, row.length);

    assertArrayEquals("a".getBytes(StandardCharsets.UTF_8), readStringField(r));
    assertArrayEquals("b".getBytes(StandardCharsets.UTF_8), readStringField(r));
    assertFalse("schema has exactly 2 fields — the rest must not surface", r.readNextField());
  }

  // --- single-byte field delimiter, with escape (escape hot loop) ----------

  /**
   * The point of escape: a delim byte preceded by escape.delim must NOT split
   * fields. Also verifies the {@code escapeCounts} bookkeeping — reading the
   * field back out requires the external-buffer copy path.
   */
  @Test
  public void testSingleByteEscapeEscapedDelimInsideField() throws Exception {
    LazySerDeParameters params = escapeParams("|", "\\");
    TypeInfo[] typeInfos = new TypeInfo[] {
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo
    };
    LazySimpleDeserializeRead r = new LazySimpleDeserializeRead(typeInfos, null, true, params);

    // "foo\|bar|baz" — the escaped '|' stays inside field 0.
    byte[] row = "foo\\|bar|baz".getBytes(StandardCharsets.UTF_8);
    r.set(row, 0, row.length);

    assertTrue(r.readNextField());
    assertTrue("escaped delim inside field forces the copy-out path",
        r.currentExternalBufferNeeded);
    byte[] buf = new byte[r.currentExternalBufferNeededLen];
    r.copyToExternalBuffer(buf, 0);
    assertArrayEquals("foo|bar".getBytes(StandardCharsets.UTF_8), buf);

    assertArrayEquals("baz".getBytes(StandardCharsets.UTF_8), readStringField(r));
  }

  /**
   * Escape path, empty last field. The endLessOne / trailing-tail branch has
   * to notice that we're at EOL and record a zero-length last field.
   */
  @Test
  public void testSingleByteEscapeEmptyLastField() throws Exception {
    LazySerDeParameters params = escapeParams("|", "\\");
    TypeInfo[] typeInfos = new TypeInfo[] {
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo
    };
    LazySimpleDeserializeRead r = new LazySimpleDeserializeRead(typeInfos, null, true, params);

    byte[] row = "one|".getBytes(StandardCharsets.UTF_8);
    r.set(row, 0, row.length);

    assertArrayEquals("one".getBytes(StandardCharsets.UTF_8), readStringField(r));
    assertTrue("empty last field on escape path must be non-null (zero-length)",
        r.readNextField());
    assertEquals(0, r.currentBytesLength);
  }

  /**
   * Escape path, missing trailing fields → NULL. Symmetric to the no-escape
   * missing-trailing test; makes sure the sentinel-fill epilogue is common
   * to both parse helpers.
   */
  @Test
  public void testSingleByteEscapeMissingTrailingFields() throws Exception {
    LazySerDeParameters params = escapeParams("|", "\\");
    TypeInfo[] typeInfos = new TypeInfo[] {
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo
    };
    LazySimpleDeserializeRead r = new LazySimpleDeserializeRead(typeInfos, null, true, params);

    byte[] row = "solo".getBytes(StandardCharsets.UTF_8);
    r.set(row, 0, row.length);

    assertArrayEquals("solo".getBytes(StandardCharsets.UTF_8), readStringField(r));
    assertFalse(r.readNextField());
    assertFalse(r.readNextField());
  }

  /**
   * Escape path with mixed INT / STRING columns and NO escape characters in
   * the payload. Exercises the "no escape, no delim" plain-byte inner branch
   * many times in a row and confirms numeric parsing still sees a
   * zero-escape field boundary.
   */
  @Test
  public void testSingleByteEscapeMixedTypesNoEscapesInPayload() throws Exception {
    LazySerDeParameters params = escapeParams("|", "\\");
    TypeInfo[] typeInfos = new TypeInfo[] {
        TypeInfoFactory.intTypeInfo,
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.longTypeInfo
    };
    LazySimpleDeserializeRead r = new LazySimpleDeserializeRead(typeInfos, null, true, params);

    byte[] row = "42|hello|9876543210".getBytes(StandardCharsets.UTF_8);
    r.set(row, 0, row.length);

    assertTrue(r.readNextField());
    assertEquals(42, r.currentInt);
    assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), readStringField(r));
    assertTrue(r.readNextField());
    assertEquals(9876543210L, r.currentLong);
  }

  /**
   * The last byte of the row is a delim: {@code endLessOne} loop stops one
   * short, then the trailing single-byte branch fires. Confirms both the
   * "last byte is separator" mini-branch and the following empty-last-field
   * epilogue produce the right startPositions.
   */
  @Test
  public void testSingleByteEscapeLastByteIsDelim() throws Exception {
    LazySerDeParameters params = escapeParams("|", "\\");
    TypeInfo[] typeInfos = new TypeInfo[] {
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo
    };
    LazySimpleDeserializeRead r = new LazySimpleDeserializeRead(typeInfos, null, true, params);

    byte[] row = "a|b|".getBytes(StandardCharsets.UTF_8);
    r.set(row, 0, row.length);

    assertArrayEquals("a".getBytes(StandardCharsets.UTF_8), readStringField(r));
    assertArrayEquals("b".getBytes(StandardCharsets.UTF_8), readStringField(r));
    assertTrue("empty tail after final delim must be non-null (zero-length)",
        r.readNextField());
    assertEquals(0, r.currentBytesLength);
  }

  /**
   * Escape char at the very last position of the row has nothing to swallow.
   * The {@code endLessOne} guard is what prevents the "escape swallows next
   * byte" step from reading past {@code end}; this test pins that behavior.
   */
  @Test
  public void testSingleByteEscapeCharAtEndOfInput() throws Exception {
    LazySerDeParameters params = escapeParams("|", "\\");
    TypeInfo[] typeInfos = new TypeInfo[] {TypeInfoFactory.stringTypeInfo };
    LazySimpleDeserializeRead r = new LazySimpleDeserializeRead(typeInfos, null, true, params);

    // Single field whose last byte is the escape char with no follower.
    byte[] row = "abc\\".getBytes(StandardCharsets.UTF_8);
    r.set(row, 0, row.length);

    // Must not throw and must not read past end. Whatever surface value the
    // reader chooses is fine as long as the length is bounded.
    assertTrue(r.readNextField());
    if (r.currentExternalBufferNeeded) {
      byte[] buf = new byte[r.currentExternalBufferNeededLen];
      r.copyToExternalBuffer(buf, 0);
      assertTrue("copy-out length must not exceed the row length",
          buf.length <= row.length);
    } else {
      assertTrue("in-place length must not exceed the row length",
          r.currentBytesLength <= row.length);
    }
  }

  // --- multi-byte field delimiter (MultiDelimitSerDe → LLAP fast path) ------

  /**
   * Three STRING columns separated by "~|" — the delimiter used by the BofA
   * MultiDelimitSerDe workload that motivated this path.
   */
  @Test
  public void testMultiByteDelimThreeStringColumns() throws Exception {
    LazySerDeParameters params = multiDelimParams("~|");
    TypeInfo[] typeInfos = new TypeInfo[] {
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo
    };
    LazySimpleDeserializeRead r = new LazySimpleDeserializeRead(typeInfos, null, true, params);

    byte[] row = "alpha~|beta~|gamma".getBytes(StandardCharsets.UTF_8);
    r.set(row, 0, row.length);

    assertArrayEquals("alpha".getBytes(StandardCharsets.UTF_8), readStringField(r));
    assertArrayEquals("beta".getBytes(StandardCharsets.UTF_8), readStringField(r));
    assertArrayEquals("gamma".getBytes(StandardCharsets.UTF_8), readStringField(r));
    assertFalse("row should have exactly 3 fields", r.readNextField());
  }

  /**
   * Mixed INT / STRING columns. Verifies that the length arithmetic charges
   * the full delim.length (not 1) between fields so numeric parsing sees the
   * correct field boundaries.
   */
  @Test
  public void testMultiByteDelimMixedTypes() throws Exception {
    LazySerDeParameters params = multiDelimParams("~|");
    TypeInfo[] typeInfos = new TypeInfo[] {
        TypeInfoFactory.intTypeInfo,
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.longTypeInfo
    };
    LazySimpleDeserializeRead r = new LazySimpleDeserializeRead(typeInfos, null, true, params);

    byte[] row = "42~|hello~|9876543210".getBytes(StandardCharsets.UTF_8);
    r.set(row, 0, row.length);

    assertTrue(r.readNextField());
    assertEquals(42, r.currentInt);

    assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), readStringField(r));

    assertTrue(r.readNextField());
    assertEquals(9876543210L, r.currentLong);
  }

  /**
   * The first byte of the delimiter ("~") appearing standalone inside a field
   * must NOT trigger a split — the tail-match on delim[1] rescues it.
   */
  @Test
  public void testMultiByteDelimFirstByteInsideField() throws Exception {
    LazySerDeParameters params = multiDelimParams("~|");
    TypeInfo[] typeInfos = new TypeInfo[] {
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo
    };
    LazySimpleDeserializeRead r = new LazySimpleDeserializeRead(typeInfos, null, true, params);

    // A bare '~' followed by non-'|' must stay inside the first field.
    byte[] row = "foo~bar~|baz".getBytes(StandardCharsets.UTF_8);
    r.set(row, 0, row.length);

    assertArrayEquals("foo~bar".getBytes(StandardCharsets.UTF_8), readStringField(r));
    assertArrayEquals("baz".getBytes(StandardCharsets.UTF_8), readStringField(r));
  }

  /**
   * The empty-tail case: last field extends to EOL and has zero length.
   * startPositions must still tabulate length 0 (not -1), so NULL isn't
   * spuriously returned.
   */
  @Test
  public void testMultiByteDelimEmptyLastField() throws Exception {
    LazySerDeParameters params = multiDelimParams("~|");
    TypeInfo[] typeInfos = new TypeInfo[] {
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo
    };
    LazySimpleDeserializeRead r = new LazySimpleDeserializeRead(typeInfos, null, true, params);

    byte[] row = "one~|".getBytes(StandardCharsets.UTF_8);
    r.set(row, 0, row.length);

    assertArrayEquals("one".getBytes(StandardCharsets.UTF_8), readStringField(r));
    assertTrue("empty last field should be non-null (zero-length)", r.readNextField());
    assertEquals(0, r.currentBytesLength);
  }

  /**
   * Missing trailing fields must land in the "startPositions filled with
   * sentinel" branch and come back as NULL — same behavior as single-byte.
   */
  @Test
  public void testMultiByteDelimMissingTrailingFields() throws Exception {
    LazySerDeParameters params = multiDelimParams("~|");
    TypeInfo[] typeInfos = new TypeInfo[] {
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo
    };
    LazySimpleDeserializeRead r = new LazySimpleDeserializeRead(typeInfos, null, true, params);

    byte[] row = "only-one".getBytes(StandardCharsets.UTF_8);
    r.set(row, 0, row.length);

    assertArrayEquals("only-one".getBytes(StandardCharsets.UTF_8), readStringField(r));
    // Missing fields → NULL.
    assertFalse(r.readNextField());
    assertFalse(r.readNextField());
  }

  /**
   * A three-byte delimiter exercises the "delim length > 2" arithmetic.
   */
  @Test
  public void testMultiByteDelimThreeByteDelimiter() throws Exception {
    LazySerDeParameters params = multiDelimParams("|~|");
    TypeInfo[] typeInfos = new TypeInfo[] {
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo
    };
    LazySimpleDeserializeRead r = new LazySimpleDeserializeRead(typeInfos, null, true, params);

    byte[] row = "a|~|bb|~|ccc".getBytes(StandardCharsets.UTF_8);
    r.set(row, 0, row.length);

    assertArrayEquals("a".getBytes(StandardCharsets.UTF_8), readStringField(r));
    assertArrayEquals("bb".getBytes(StandardCharsets.UTF_8), readStringField(r));
    assertArrayEquals("ccc".getBytes(StandardCharsets.UTF_8), readStringField(r));
  }

  /**
   * Multi-byte delim combined with escape.delim is not supported in the fast
   * path — LazySimpleDeserializeRead must reject the combination at
   * construction time so we never silently diverge from
   * MultiDelimitSerDe.parseMultiDelimit (which itself ignores escape at the
   * top level).
   */
  @Test
  public void testMultiByteDelimRejectsEscape() throws Exception {
    Properties props = new Properties();
    props.setProperty(serdeConstants.FIELD_DELIM, "~|");
    props.setProperty(serdeConstants.SERIALIZATION_FORMAT, "~|");
    props.setProperty(serdeConstants.ESCAPE_CHAR, "\\");
    LazySerDeParameters params = new LazySerDeParameters(new HiveConf(), props,
        LazySimpleSerDe.class.getName());
    params.setFieldDelimMulti("~|".getBytes(StandardCharsets.UTF_8));

    TypeInfo[] typeInfos = new TypeInfo[]{TypeInfoFactory.stringTypeInfo};
    try {
      new LazySimpleDeserializeRead(typeInfos, null, true, params);
      fail("expected RuntimeException for multi-byte delim + escape.delim");
    } catch (RuntimeException expected) {
      assertTrue("unexpected message: " + expected.getMessage(),
          expected.getMessage().contains("Multi-byte field delimiter"));
    }
  }

  /**
   * When the table is a plain LazySimpleSerDe (setFieldDelimMulti never called
   * on the params), LazySimpleDeserializeRead must select the specialized
   * single-byte hot loop in {@code topLevelParse()}. The branch is gated on
   * {@code fieldDelimMulti == null}, so we prove path selection by reflecting
   * on the reader's internal state after a successful parse:
   * <ul>
   *   <li>{@code fieldDelimMulti} is {@code null} — the {@code if} in
   *       {@code topLevelParse()} evaluated to the single-byte branch;</li>
   *   <li>{@code topLevelSeparatorLen == 1} — the length arithmetic
   *       (both in the sentinel fill and in {@code readField}) uses the
   *       single-byte constant, not the multi-byte {@code dlen}.</li>
   * </ul>
   * A correct parse of the row confirms the branch actually ran.
   */
  @Test
  public void testSingleByteDelimHitsSpecializedHotPath() throws Exception {
    Properties props = new Properties();
    props.setProperty(serdeConstants.FIELD_DELIM, "|");
    props.setProperty(serdeConstants.SERIALIZATION_FORMAT, "|");
    LazySerDeParameters params = new LazySerDeParameters(new HiveConf(), props,
        LazySimpleSerDe.class.getName());
    // Intentionally do NOT call setFieldDelimMulti — this is a LazySimple table.

    TypeInfo[] typeInfos = new TypeInfo[] {
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo
    };
    LazySimpleDeserializeRead r = new LazySimpleDeserializeRead(typeInfos, null, true, params);

    byte[] row = "alpha|beta|gamma".getBytes(StandardCharsets.UTF_8);
    r.set(row, 0, row.length);
    assertArrayEquals("alpha".getBytes(StandardCharsets.UTF_8), readStringField(r));
    assertArrayEquals("beta".getBytes(StandardCharsets.UTF_8), readStringField(r));
    assertArrayEquals("gamma".getBytes(StandardCharsets.UTF_8), readStringField(r));

    // Path-selection proof: the reader's fieldDelimMulti stayed null and its
    // topLevelSeparatorLen is 1 — the only code path in topLevelParse() that
    // could have produced the correct parse above.
    assertNull("fieldDelimMulti must be null on the LazySimple fast path",
        readPrivateField(r, "fieldDelimMulti"));
    assertEquals("topLevelSeparatorLen must be 1 on the LazySimple fast path",
        1, ((Integer) readPrivateField(r, "topLevelSeparatorLen")).intValue());
  }

  private static LazySerDeParameters singleDelimParams(String delim) throws Exception {
    Properties props = new Properties();
    props.setProperty(serdeConstants.FIELD_DELIM, delim);
    props.setProperty(serdeConstants.SERIALIZATION_FORMAT, delim);
    return new LazySerDeParameters(new HiveConf(), props, LazySimpleSerDe.class.getName());
  }

  private static LazySerDeParameters escapeParams(String delim, String escape) throws Exception {
    Properties props = new Properties();
    props.setProperty(serdeConstants.FIELD_DELIM, delim);
    props.setProperty(serdeConstants.SERIALIZATION_FORMAT, delim);
    props.setProperty(serdeConstants.ESCAPE_CHAR, escape);
    return new LazySerDeParameters(new HiveConf(), props, LazySimpleSerDe.class.getName());
  }

  private static LazySerDeParameters multiDelimParams(String delim) throws Exception {
    Properties props = new Properties();
    props.setProperty(serdeConstants.FIELD_DELIM, delim);
    props.setProperty(serdeConstants.SERIALIZATION_FORMAT, delim);
    LazySerDeParameters p = new LazySerDeParameters(new HiveConf(), props,
        LazySimpleSerDe.class.getName());
    p.setFieldDelimMulti(delim.getBytes(StandardCharsets.UTF_8));
    return p;
  }

  private static byte[] readStringField(LazySimpleDeserializeRead r) throws Exception {
    assertTrue("expected non-null field", r.readNextField());
    int len = r.currentBytesLength;
    byte[] out = new byte[len];
    System.arraycopy(r.currentBytes, r.currentBytesStart, out, 0, len);
    return out;
  }

  private static Object readPrivateField(Object target, String name) throws Exception {
    Field f = LazySimpleDeserializeRead.class.getDeclaredField(name);
    f.setAccessible(true);
    return f.get(target);
  }
}
