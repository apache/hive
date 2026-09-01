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

  // --- multi-byte field delimiter (MultiDelimitSerDe → LLAP fast path) ------

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

    TypeInfo[] typeInfos = new TypeInfo[] { TypeInfoFactory.stringTypeInfo };
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

  private static Object readPrivateField(Object target, String name) throws Exception {
    Field f = LazySimpleDeserializeRead.class.getDeclaredField(name);
    f.setAccessible(true);
    return f.get(target);
  }
}
