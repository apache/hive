package org.apache.hive.common.util;

import static org.junit.Assert.*;
import org.junit.Test;

import java.sql.Date;

public class TestDateParser {
  DateParser parser = new DateParser();
  Date date = new Date(0);

  void checkValidCase(String strValue, Date expected) {
    Date dateValue = parser.parseDate(strValue);
    assertEquals(expected, dateValue);

    assertTrue(parser.parseDate(strValue, date));
    assertEquals(expected, date);
  }

  void checkInvalidCase(String strValue) {
    Date dateValue = parser.parseDate(strValue);
    assertNull(dateValue);

    assertFalse(parser.parseDate(strValue, date));
  }

  @Test
  public void testValidCases() throws Exception {
    checkValidCase("1945-12-31", Date.valueOf("1945-12-31"));
    checkValidCase("1946-01-01", Date.valueOf("1946-01-01"));
    checkValidCase("2001-11-12", Date.valueOf("2001-11-12"));
    checkValidCase("0004-05-06", Date.valueOf("0004-05-06"));
    checkValidCase("1678-09-10", Date.valueOf("1678-09-10"));
    checkValidCase("9999-10-11", Date.valueOf("9999-10-11"));

    // Timestamp strings should parse ok
    checkValidCase("2001-11-12 01:02:03", Date.valueOf("2001-11-12"));

    // Leading spaces
    checkValidCase(" 1946-01-01", Date.valueOf("1946-01-01"));
    checkValidCase(" 2001-11-12 01:02:03", Date.valueOf("2001-11-12"));

    // Current date parsing is lenient
    checkValidCase("2001-13-12", Date.valueOf("2002-01-12"));
    checkValidCase("2001-11-31", Date.valueOf("2001-12-01"));
  }

  @Test
  public void testInvalidCases() throws Exception {
    checkInvalidCase("2001");
    checkInvalidCase("2001-01");
    checkInvalidCase("abc");
    checkInvalidCase(" 2001 ");
    checkInvalidCase("a2001-01-01");
  }
}
