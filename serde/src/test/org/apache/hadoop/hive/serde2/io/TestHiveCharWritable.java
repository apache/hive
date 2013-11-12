package org.apache.hadoop.hive.serde2.io;

import junit.framework.TestCase;
import org.apache.hadoop.hive.common.type.HiveChar;

public class TestHiveCharWritable extends TestCase {
  public void testConstructor() throws Exception {
    HiveCharWritable hcw1 = new HiveCharWritable(new HiveChar("abc", 5));
    assertEquals("abc  ", hcw1.toString());

    HiveCharWritable hcw2 = new HiveCharWritable(hcw1);
    assertEquals("abc  ", hcw2.toString());
  }

  public void testSet() throws Exception {
    HiveCharWritable hcw1 = new HiveCharWritable();

    HiveChar hc1 = new HiveChar("abcd", 8);
    hcw1.set(hc1);
    assertEquals("abcd    ", hcw1.toString());

    hcw1.set(hc1, 10);
    assertEquals("abcd      ", hcw1.toString());

    hcw1.set(hc1, 2);
    assertEquals("ab", hcw1.toString());

    // copy whole value for strings
    hcw1.set("abcd");
    assertEquals("abcd", hcw1.toString());

    // whole value is copied, including spaces
    hcw1.set("abcd ");
    assertEquals("abcd ", hcw1.toString());

    hcw1.set("abcd", 10);
    assertEquals("abcd      ", hcw1.toString());

    hcw1.set("abcd", 2);
    assertEquals("ab", hcw1.toString());

    HiveCharWritable hcw2 = new HiveCharWritable(hc1);
    hcw1.set(hcw2);
    assertEquals("abcd    ", hcw1.toString());

    hcw1.set(hcw2, 10);
    assertEquals("abcd      ", hcw1.toString());
    assertEquals("abcd      ", hcw1.getTextValue().toString());

    hcw1.set(hcw2, 2);
    assertEquals("ab", hcw1.toString());
    assertEquals("ab", hcw1.getTextValue().toString());
  }

  public void testGetHiveChar() throws Exception {
    HiveCharWritable hcw = new HiveCharWritable();
    hcw.set("abcd", 10);
    assertEquals("abcd      ", hcw.getHiveChar().toString());
  }

  public void testGetCharacterLength() throws Exception {
    HiveCharWritable hcw = new HiveCharWritable();
    hcw.set("abcd", 10);
    assertEquals(4, hcw.getCharacterLength());
  }

  public void testEnforceMaxLength() {
    HiveCharWritable hcw1 = new HiveCharWritable();
    hcw1.set("abcdefghij", 10);
    assertEquals("abcdefghij", hcw1.toString());
    hcw1.enforceMaxLength(12);
    assertEquals("abcdefghij  ", hcw1.toString());
    hcw1.enforceMaxLength(5);
    assertEquals("abcde", hcw1.toString());
  }

  public void testComparison() throws Exception {
    HiveCharWritable hcw1 = new HiveCharWritable();
    HiveCharWritable hcw2 = new HiveCharWritable();

    // same string
    hcw1.set("abcd", 4);
    hcw2.set("abcd", 4);
    assertEquals(hcw1, hcw2);
    assertEquals(hcw2, hcw1);
    assertEquals(0, hcw1.compareTo(hcw2));
    assertEquals(0, hcw2.compareTo(hcw1));

    // unequal strings
    hcw1.set("abcd", 4);
    hcw2.set("abc", 4);
    assertFalse(hcw1.equals(hcw2));
    assertFalse(hcw2.equals(hcw1));
    assertFalse(0 == hcw1.compareTo(hcw2));
    assertFalse(0 == hcw2.compareTo(hcw1));

    // trailing spaces are not significant
    hcw1.set("abcd ", 10);
    hcw2.set("abcd", 4);
    assertEquals("abcd      ", hcw1.toString());
    assertEquals(hcw1, hcw2);
    assertEquals(hcw2, hcw1);
    assertEquals(0, hcw1.compareTo(hcw2));
    assertEquals(0, hcw2.compareTo(hcw1));

    // leading spaces are significant
    hcw1.set(" abcd", 5);
    hcw2.set("abcd", 5);
    assertFalse(hcw1.equals(hcw2));
    assertFalse(hcw2.equals(hcw1));
    assertFalse(0 == hcw1.compareTo(hcw2));
    assertFalse(0 == hcw2.compareTo(hcw1));
  }
}
