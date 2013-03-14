package org.apache.hadoop.hive.ql.cube.parse;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.log4j.Logger;

import com.google.common.base.Strings;

public class DateUtils {
  public static final Logger LOG = Logger.getLogger(DateUtils.class);

  /*
   * NOW -> new java.util.Date()
   * NOW-7DAY -> a date one week earlier
   * NOW (+-) <NUM>UNIT
   * or Hardcoded dates in  DD-MM-YYYY hh:mm:ss,sss
   */
  public static final String RELATIVE = "(now){1}";
  public static final Pattern P_RELATIVE = Pattern.compile(RELATIVE, Pattern.CASE_INSENSITIVE);

  public static final String WSPACE = "\\s+";
  public static final Pattern P_WSPACE = Pattern.compile(WSPACE);

  public static final String SIGNAGE = "\\+|\\-";
  public static final Pattern P_SIGNAGE = Pattern.compile(SIGNAGE);

  public static final String QUANTITY = "\\d+";
  public static final Pattern P_QUANTITY = Pattern.compile(QUANTITY);

  public static final String UNIT = "year|month|week|day|hour|minute|second";
  public static final Pattern P_UNIT = Pattern.compile(UNIT, Pattern.CASE_INSENSITIVE);


  public static final String RELDATE_VALIDATOR_STR = RELATIVE
      + "(" + WSPACE + ")?"
      + "((" + SIGNAGE +")"
      + "(" + WSPACE + ")?"
      + "(" + QUANTITY + ")(" + UNIT + ")){0,1}"
      +"(s?)";

  public static final Pattern RELDATE_VALIDATOR = Pattern.compile(RELDATE_VALIDATOR_STR, Pattern.CASE_INSENSITIVE);

  public static final String ABSDATE_FMT = "dd-MMM-yyyy HH:mm:ss,SSS Z";
  public static final SimpleDateFormat ABSDATE_PARSER = new SimpleDateFormat(ABSDATE_FMT);

  public static String formatDate(Date dt) {
    return ABSDATE_PARSER.format(dt);
  }

  public static Date resolveDate(String str, Date now) throws HiveException {
    if (RELDATE_VALIDATOR.matcher(str).matches()) {
      return resolveRelativeDate(str, now);
    } else {
      try {
        return ABSDATE_PARSER.parse(str);
      } catch (ParseException e) {
        LOG.error("Invalid date format. expected only " + ABSDATE_FMT + " date provided:" + str, e);
        throw new HiveException("Date parsing error. expected format " + ABSDATE_FMT
            + ", date provided: " + str
            + ", failed because: " + e.getMessage());
      }
    }
  }

  private static Date resolveRelativeDate(String str, Date now) throws HiveException {
    if (Strings.isNullOrEmpty(str)) {
      throw new HiveException("date value cannot be null or empty:" + str);
    }
    // Get rid of whitespace
    String raw = str.replaceAll(WSPACE, "").replaceAll(RELATIVE, "");

    if (raw.isEmpty()) { // String is just "now"
      return now;
    }

    Matcher qtyMatcher = P_QUANTITY.matcher(raw);
    int qty = 1;
    if (qtyMatcher.find() && true) {
      qty =  Integer.parseInt(qtyMatcher.group());
    }

    Matcher signageMatcher = P_SIGNAGE.matcher(raw);
    if (signageMatcher.find()) {
      String sign = signageMatcher.group();
      if ("-".equals(sign)) {
        qty = -qty;
      }
    }

    Matcher unitMatcher = P_UNIT.matcher(raw);
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(now);

    if (unitMatcher.find()) {
      String unit = unitMatcher.group().toLowerCase();
      if ("year".equals(unit)) {
        calendar.add(Calendar.YEAR, qty);
      } else if ("month".equals(unit)) {
        calendar.add(Calendar.MONTH, qty);
      } else if ("week".equals(unit)) {
        calendar.add(Calendar.DAY_OF_MONTH, 7 * qty);
      } else if ("day".equals(unit)) {
        calendar.add(Calendar.DAY_OF_MONTH, qty);
      } else if ("hour".equals(unit)) {
        calendar.add(Calendar.HOUR_OF_DAY, qty);
      } else if ("minute".equals(unit)) {
        calendar.add(Calendar.MINUTE, qty);
      } else if ("second".equals(unit)) {
        calendar.add(Calendar.SECOND, qty);
      } else {
        throw new HiveException("invalid time unit: "+ unit);
      }
    }

    return calendar.getTime();
  }
}
