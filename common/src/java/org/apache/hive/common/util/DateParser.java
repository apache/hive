package org.apache.hive.common.util;

import java.sql.Date;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;

/**
 * Date parser class for Hive.
 */
public class DateParser {
  SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
  ParsePosition pos = new ParsePosition(0);

  public Date parseDate(String strValue) {
    Date result = new Date(0);
    if (parseDate(strValue, result)) {
      return result;
    }
    return null;
  }

  public boolean parseDate(String strValue, Date result) {
    pos.setIndex(0);
    java.util.Date parsedVal = formatter.parse(strValue, pos);
    if (parsedVal == null) {
      return false;
    }
    result.setTime(parsedVal.getTime());
    return true;
  }
}
