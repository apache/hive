package org.apache.hive.common.util;

import java.sql.Date;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;

/**
 * Date parser class for Hive.
 */
public class DateParser {
  private final SimpleDateFormat formatter;
  private final ParsePosition pos;
  public DateParser() {
    formatter = new SimpleDateFormat("yyyy-MM-dd");
    // TODO: ideally, we should set formatter.setLenient(false);
    pos = new ParsePosition(0);
  }

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
