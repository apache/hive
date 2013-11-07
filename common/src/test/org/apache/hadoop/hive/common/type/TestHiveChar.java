package org.apache.hadoop.hive.common.type;

import junit.framework.TestCase;

public class TestHiveChar extends TestCase {

  public void testBasic() {
    HiveChar hc = new HiveChar("abc", 10);
    assertEquals("abc       ", hc.toString());
    assertEquals("abc       ", hc.getPaddedValue());
    assertEquals("abc", hc.getStrippedValue());
    assertEquals(3, hc.getCharacterLength());

    hc.setValue("abc123");
    assertEquals("abc123", hc.toString());
    assertEquals("abc123", hc.getPaddedValue());
    assertEquals("abc123", hc.getStrippedValue());
    assertEquals(6, hc.getCharacterLength());

    hc.setValue("xyz", 15);
    assertEquals("xyz            ", hc.toString());
    assertEquals("xyz            ", hc.getPaddedValue());
    assertEquals("xyz", hc.getStrippedValue());
    assertEquals(3, hc.getCharacterLength());

    hc.setValue("abc   ", 5);
    assertEquals("abc  ", hc.toString());
    assertEquals("abc", hc.getStrippedValue());
    assertEquals(3, hc.getCharacterLength());
  }

  public void testStringLength() {
    HiveChar hc = new HiveChar();

    hc.setValue("0123456789", 5);
    assertEquals("01234", hc.toString());

    hc.setValue("0123456789", 10);
    assertEquals("0123456789", hc.toString());

    hc.setValue("0123456789", 15);
    assertEquals("0123456789     ", hc.toString());
  }

  public void testComparison() {
    HiveChar hc1 = new HiveChar();
    HiveChar hc2 = new HiveChar();

    // Identical strings
    hc1.setValue("abc", 3);
    hc2.setValue("abc", 3);
    assertEquals(hc1, hc2);
    assertEquals(hc2, hc1);
    assertEquals(0, hc1.compareTo(hc2));
    assertEquals(0, hc2.compareTo(hc1));

    // Unequal strings
    hc1.setValue("abc", 3);
    hc1.setValue("123", 3);
    assertFalse(hc1.equals(hc2));
    assertFalse(hc2.equals(hc1));
    assertFalse(0 == hc1.compareTo(hc2));
    assertFalse(0 == hc2.compareTo(hc1));

    // Trailing spaces are not significant
    hc1.setValue("abc", 3);
    hc2.setValue("abc", 5);
    assertEquals("abc", hc1.toString());
    assertEquals("abc  ", hc2.toString());
    assertEquals(hc1, hc2);
    assertEquals(hc2, hc1);
    assertEquals(0, hc1.compareTo(hc2));
    assertEquals(0, hc2.compareTo(hc1));

    // Leading space is significant
    hc1.setValue(" abc", 4);
    hc2.setValue("abc", 4);
    assertFalse(hc1.equals(hc2));
    assertFalse(hc2.equals(hc1));
    assertFalse(0 == hc1.compareTo(hc2));
    assertFalse(0 == hc2.compareTo(hc1));
  }
}
