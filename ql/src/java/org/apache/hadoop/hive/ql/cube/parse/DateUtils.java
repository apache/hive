package org.apache.hadoop.hive.ql.cube.parse;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
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

  public static String YEAR_FMT = "[0-9]{4}";
  public static String MONTH_FMT = YEAR_FMT + "-[0-9]{2}";
  public static String DAY_FMT = MONTH_FMT + "-[0-9]{2}";
  public static String HOUR_FMT = DAY_FMT + " [0-9]{2}";
  public static String MINUTE_FMT = HOUR_FMT + ":[0-9]{2}";
  public static String SECOND_FMT = MINUTE_FMT + ":[0-9]{2}";
  public static final String ABSDATE_FMT = "yyyy-MM-dd HH:mm:ss,SSS";
  public static final SimpleDateFormat ABSDATE_PARSER = new SimpleDateFormat(ABSDATE_FMT);

  public static String formatDate(Date dt) {
    return ABSDATE_PARSER.format(dt);
  }

  public static String getAbsDateFormatString(String str) {
    if (str.matches(YEAR_FMT)) {
      return str + "-01-01 00:00:00,000";
    } else if (str.matches(MONTH_FMT)) {
      return str + "-01 00:00:00,000";
    } else if (str.matches(DAY_FMT)) {
      return str + " 00:00:00,000";
    } else if (str.matches(HOUR_FMT)) {
      return str + ":00:00,000";
    } else if (str.matches(MINUTE_FMT)) {
      return str + ":00,000";
    } else if (str.matches(SECOND_FMT)) {
      return str + ",000";
    } else if (str.matches(ABSDATE_FMT)) {
      return str;
    }
    throw new IllegalArgumentException("Unsupported formatting for date" + str);
  }

  public static Date resolveDate(String str, Date now) throws HiveException {
    if (RELDATE_VALIDATOR.matcher(str).matches()) {
      return resolveRelativeDate(str, now);
    } else {
      try {
        return ABSDATE_PARSER.parse(getAbsDateFormatString(str));
      } catch (ParseException e) {
        LOG.error("Invalid date format. expected only " + ABSDATE_FMT
            + " date provided:" + str, e);
        throw new HiveException("Date parsing error. expected format "
            + ABSDATE_FMT
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

  public static Date getCeilDate(Date fromDate, UpdatePeriod interval) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(fromDate);
    boolean hasFraction = false;
    switch (interval) {
    case YEARLY :
      if (cal.get(Calendar.MONTH) != 1) {
        hasFraction = true;
        break;
      }
    case MONTHLY :
      if (cal.get(Calendar.DAY_OF_MONTH) != 1) {
        hasFraction = true;
        break;
      }
    case WEEKLY :
      if (cal.get(Calendar.DAY_OF_WEEK) != 1) {
        hasFraction = true;
        break;
      }
    case DAILY :
      if (cal.get(Calendar.HOUR_OF_DAY) != 0) {
        hasFraction = true;
        break;
      }
    case HOURLY :
      if (cal.get(Calendar.MINUTE) != 0) {
        hasFraction = true;
        break;
      }
    case MINUTELY :
      if (cal.get(Calendar.SECOND) != 0) {
        hasFraction = true;
        break;
      }
    case SECONDLY :
      if (cal.get(Calendar.MILLISECOND) != 0) {
        hasFraction = true;
        break;
      }
    }

    if (hasFraction) {
      cal.add(interval.calendarField(), 1);
      return getFloorDate(cal.getTime(), interval);
    } else {
      return fromDate;
    }
  }

  public static Date getFloorDate(Date toDate, UpdatePeriod interval) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(toDate);
    switch (interval) {
    case YEARLY :
      cal.set(Calendar.MONTH, 1);
    case MONTHLY :
      cal.set(Calendar.DAY_OF_MONTH, 1);
    case DAILY :
      cal.set(Calendar.HOUR_OF_DAY, 0);
    case HOURLY :
      cal.set(Calendar.MINUTE, 0);
    case MINUTELY :
      cal.set(Calendar.SECOND, 0);
    case SECONDLY :
      break;
    case WEEKLY :
      cal.set(Calendar.DAY_OF_WEEK, 1);
      cal.set(Calendar.HOUR_OF_DAY, 0);
      cal.set(Calendar.MINUTE, 0);
      cal.set(Calendar.SECOND, 0);
      break;
    }
    System.out.println("Date:" + toDate + " Floordate for interval:" + interval + " is " + cal.getTime());
    return cal.getTime();
  }

  public static int getNumberofDaysInMonth(Date date) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    return calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
  }
}
