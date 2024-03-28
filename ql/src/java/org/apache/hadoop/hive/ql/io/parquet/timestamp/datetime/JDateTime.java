// Copyright (c) 2003-2014, Jodd Team (jodd.org). All Rights Reserved.

package org.apache.hadoop.hive.ql.io.parquet.timestamp.datetime;

import java.util.TimeZone;

/**
 * Universal all-in-one date and time class that uses Astronomical Julian
 * Dates for time calculations. Guaranteed precision for all manipulations
 * and calculations is up to 1 ms (0.001 sec).
 *
 * <p>
 * The Julian day or Julian day number (JDN) is the (integer) number of days that
 * have elapsed since Monday, January 1, 4713 BC in the proleptic Julian calendar 1.
 * That day is counted as Julian day zero. Thus the multiples of 7 are Mondays.
 * Negative values can also be used.
 *
 * <p>
 * The Julian Date (JD) is the number of days (with decimal fraction of the day) that
 * have elapsed since 12 noon Greenwich Mean Time (UT or TT) of that day.
 * Rounding to the nearest integer gives the Julian day number.
 *
 * <p>
 * <code>JDateTime</code> contains date/time information for current day. By
 * default, behaviour and formats are set to ISO standard, although this may
 * be changed.
 *
 * <p>
 * <code>JDateTime</code> can be set in many different ways by using <code>setXxx()</code>
 * methods or equivalent constructors. Moreover, date time information may be loaded from an instance
 * of any available java date-time class. This functionality can be easily
 * enhanced for any custom date/time class. Furthermore, <code>JDateTime</code>
 * can be converted to any such date/time class.
 *
 * <p>
 * Rolling dates with <code>JDateTime</code> is easy. For this
 * <code>JDateTime</code> contains many <code>addXxx()</code> methods. Time can be added
 * or subtracted with any value or more values at once. All combinations are
 * valid. Calculations also performs month fixes by default.
 *
 * <p>
 * <code>JDateTime</code> behaviour is set by several attributes (or
 * parameters). Each one contains 2 values: one is the default value, used by
 * all instances of <code>JDateTime</code> and the other one is just for a
 * specific instance of <code>JDateTime</code>. This means that it is
 * possible to set behaviour of all instances at once or of one particular
 * instance.
 *
 * <p>
 * Bellow is the list of behaviour attributes:
 *
 * <ul>
 *
 * <li>monthFix - since months do not have the same number of days, adding
 * months and years may be calculated in two different ways: with or
 * without month fix. when month fix is on, <code>JDateTime</code> will
 * additionally fix all time adding and fix the date. For example, adding
 * one month to 2003-01-31 will give 2003-02-28 and not 2003-03-03.
 * By default, monthFix is turned on and set to <code>true</code>.
 * </li>
 *
 * <li>locale - current locale, used for getting names during formatting the
 * output string.
 * </li>
 *
 * <li>timezone - current timezone</li>
 *
 * <li>format - is String that describes how time is converted and parsed to
 * and from a String. Default format matches ISO standard. An instance of
 * <code>JdtFormatter</code> parses and uses this template.</li>
 *
 * <li>week definition - is used for specifying the definition of the week.
 * Week is defined with first day of the week and with the must have day. A
 * must have day is a day that must exist in the 1st week of the year. For
 * example, default value is Thursday (4) as specified by ISO standard.
 * Alternatively, instead of must have day, minimal days of week may be used,
 * since this two numbers are in relation.
 * </li>
 *
 * </ul>
 *
 * Optimization: although based on heavy calculations, <code>JDateTime</code>
 * works significantly faster then java's <code>Calendar</code>s. Since
 * <code>JDateTime</code> doesn't use lazy initialization, setXxx() method is
 * slower. However, this doesn't have much effect to the global performances:
 * settings are usually not used without gettings:) As soon as any other method is
 * used (getXxx() or addXxx()) performances of <code>JDateTime</code> becomes
 * significantly better.
 *
 * <p>
 * Year 1582 is (almost:) working: after 1582-10-04 (Thursday) is 1582-10-15 (Friday).
 * Users must be aware of this when doing time rolling before across this period.
 *
 * <p>
 * More info: <a href="http://en.wikipedia.org/wiki/Julian_Date">Julian Date on Wikipedia</a>
 */
public class JDateTime {

	// day of week names
	public static final int MONDAY 		= 1;

	/**
	 * {@link DateTimeStamp} for current date.
	 */
	protected DateTimeStamp time = new DateTimeStamp();

	/**
	 * Day of week, range: [1-7] == [Monday - Sunday]
	 */
	protected int dayofweek;

	/**
	 * Day of year, range: [1-365] or [1-366]
	 */
	protected int dayofyear;

	/**
	 * Leap year flag.
	 */
	protected boolean leap;

	/**
	 * Week of year, range: [1-52] or [1-53]
	 */
	protected int weekofyear;

	/**
	 * Week of month.
	 */
	protected int weekofmonth;

	/**
	 * Current Julian Date.
	 */
	protected JulianDateStamp jdate;


	// ---------------------------------------------------------------- some precalculated times

	/**
	 * Sets current Julian Date. This is the core of the JDateTime class and it
	 * is used by all other classes. This method performs all calculations
	 * required for whole class.
	 *
	 * @param jds    current julian date
	 */
	public void setJulianDate(JulianDateStamp jds) {
		setJdOnly(jds.clone());
		calculateAdditionalData();
	}

	/**
	 * Returns JDN. Note that JDN is not equal to integer part of julian date. It is calculated by
	 * rounding to the nearest integer.
	 */
	public int getJulianDayNumber() {
		return jdate.getJulianDayNumber();
	}

	/**
	 * Internal method for calculating various data other then date/time.
	 */
	private void calculateAdditionalData() {
		this.leap = TimeUtil.isLeapYear(time.year);
		this.dayofweek = calcDayOfWeek();
		this.dayofyear = calcDayOfYear();
		this.weekofyear = calcWeekOfYear(firstDayOfWeek, mustHaveDayOfFirstWeek);
		this.weekofmonth = calcWeekNumber(time.day, this.dayofweek);
	}

	/**
	 * Internal method that just sets the time stamp and not all other additional
	 * parameters. Used for faster calculations only and only by main core
	 * set/add methods.
	 *
	 * @param jds    julian date
	 */
	private void setJdOnly(JulianDateStamp jds) {
		jdate = jds;
		time = TimeUtil.fromJulianDate(jds);
	}

	/**
	 * Core method that sets date and time. All others set() methods use this
	 * one. Milliseconds are truncated after 3rd digit.
	 *
	 * @param year   year to set
	 * @param month  month to set
	 * @param day    day to set
	 * @param hour   hour to set
	 * @param minute minute to set
	 * @param second second to set
	 */
	public void set(int year, int month, int day, int hour, int minute, int second, int millisecond) {

		// fix seconds fractions because of float point arithmetic
		//second = ((int) second) + ((int) ((second - (int)second) * 1000 + 1e-9) / 1000.0);
/*
		double ms = (second - (int)second) * 1000;
		if (ms > 999) {
			ms = 999;
		} else {
			ms += 1.0e-9;
		}
		second = ((int) second) + ((int) ms / 1000.0);
*/
		jdate = TimeUtil.toJulianDate(year, month, day, hour, minute, second, millisecond);

		// if given time is valid it means that there is no need to calculate it
		// again from already calculated Julian date. however, it is still
		// necessary to fix milliseconds to match the value that would be
		// calculated as setJulianDate() is used. This fix only deals with the
		// time, not doing the complete and more extensive date calculation.
		// this means that method works faster when regular date is specified.
/*
		if (TimeUtil.isValidDateTime(year, month, day, hour, minute, second, millisecond)) {

			int ka = (int)(jdate.fraction + 0.5);
			double frac = jdate.fraction + 0.5 - ka + 1.0e-10;

			// hour with minute and second included as fraction
			double d_hour = frac * 24.0;

			// minute with second included as a fraction
			double d_minute = (d_hour - (int)d_hour) * 60.0;

			second = (d_minute - (int)d_minute) * 60.0;

			// fix calculation errors
			second = ((int) (second * 10000) + 0.5) / 10000.0;

			time.year = year; time.month = month; time.day = day;
			time.hour = hour; time.minute = minute; time.second = second;
			setParams();
		} else {
			setJulianDate(jdate);
		}
*/
		if (TimeUtil.isValidDateTime(year, month, day, hour, minute, second, millisecond)) {
			time.year = year; time.month = month; time.day = day;
			time.hour = hour; time.minute = minute; time.second = second;
			time.millisecond = millisecond;
			calculateAdditionalData();
		} else {
			setJulianDate(jdate);
		}

	}

	// ---------------------------------------------------------------- core calculations

	/**
	 * Calculates day of week.
	 */
	private int calcDayOfWeek() {
		int jd = (int)(jdate.doubleValue() + 0.5);
		return (jd % 7) + 1;
		//return (jd + 1) % 7;		// return 0 (Sunday), 1 (Monday),...
	}

	private static final int NUM_DAYS[] = {-1, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};		// 1-based
	private static final int LEAP_NUM_DAYS[] = {-1, 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335};	// 1-based

	/**
	 * Calculates day of year.
	 */
	private int calcDayOfYear() {
		if (leap == true) {
			return LEAP_NUM_DAYS[time.month] + time.day;
		}
		return NUM_DAYS[time.month] + time.day;
	}


	/**
	 * Calculates week of year. Based on:
	 * "Algorithm for Converting Gregorian Dates to ISO 8601 Week Date"
	 * by Rick McCarty, 1999
	 *
	 * @param start  first day of week
	 * @param must   must have day of week
	 *
	 * @return week of year number
	 */
	private int calcWeekOfYear(int start, int must) {

		// is modification required?
		// modification is a fix for the days of year because of the different
		// starting day of week. when modification is required, one week is added
		// or subtracted to the current day, so calculation of the week of year
		// would be correct.
		int delta = 0;
		if (start <= this.dayofweek) {
			if (must < start) {
				delta = 7;
			}
		} else {
			if (must >= start) {
				delta = -7;
			}
		}

		int jd = (int)(jdate.doubleValue() + 0.5) + delta;
		int WeekDay = (jd % 7) + 1;

		int time_year = time.year;
		int DayOfYearNumber = this.dayofyear + delta;
		if (DayOfYearNumber < 1) {
			time_year--;
			DayOfYearNumber = TimeUtil.isLeapYear(time_year) ? 366 + DayOfYearNumber: 365 + DayOfYearNumber;
		} else if (DayOfYearNumber > (this.leap ? 366 : 365)) {
			DayOfYearNumber = this.leap ? DayOfYearNumber - 366: DayOfYearNumber - 365;
			time_year++;
		}

		// modification, if required, is finished. proceed to the calculation.

		int firstDay = jd - DayOfYearNumber + 1;
		int Jan1WeekDay = (firstDay % 7) + 1;

		// find if the date falls in YearNumber Y - 1 set WeekNumber to 52 or 53
		int YearNumber = time_year;
		int WeekNumber = 52;
		if ((DayOfYearNumber <= (8 - Jan1WeekDay)) && (Jan1WeekDay > must)) {
			YearNumber--;
			if ((Jan1WeekDay == must + 1) || ( (Jan1WeekDay == must + 2) && (TimeUtil.isLeapYear(YearNumber)) ) ) {
				WeekNumber = 53;
			}
		}

		// set WeekNumber to 1 to 53 if date falls in YearNumber
		int m = 365;
		if (YearNumber == time_year) {
			if (TimeUtil.isLeapYear(time_year) == true) {
				m = 366;
			}
			if ((m - DayOfYearNumber) < (must - WeekDay)) {
				YearNumber = time_year + 1;
				WeekNumber = 1;
			}
		}

		if (YearNumber == time_year) {
			int n = DayOfYearNumber + (7 - WeekDay) + (Jan1WeekDay - 1);
			WeekNumber = n / 7;
			if (Jan1WeekDay > must) {
				WeekNumber -= 1;
			}
		}
		return WeekNumber;
	}

	/**
	 * Return the week number of a day, within a period. This may be the week number in
	 * a year, or the week number in a month. Usually this will be a value >= 1, but if
	 * some initial days of the period are excluded from week 1, because
	 * minimalDaysInFirstWeek is > 1, then the week number will be zero for those
	 * initial days. Requires the day of week for the given date in order to determine
	 * the day of week of the first day of the period.
	 *
	 * @param dayOfPeriod
	 *                  Day-of-year or day-of-month. Should be 1 for first day of period.
	 * @param dayOfWeek day of week
	 *
	 * @return Week number, one-based, or zero if the day falls in part of the
	 *         month before the first week, when there are days before the first
	 *         week because the minimum days in the first week is more than one.
	 */
	private int calcWeekNumber(int dayOfPeriod, int dayOfWeek) {
		// Determine the day of the week of the first day of the period
		// in question (either a year or a month).  Zero represents the
		// first day of the week on this calendar.
		int periodStartDayOfWeek = (dayOfWeek - firstDayOfWeek - dayOfPeriod + 1) % 7;
		if (periodStartDayOfWeek < 0) {
			periodStartDayOfWeek += 7;
		}

		// Compute the week number.  Initially, ignore the first week, which
		// may be fractional (or may not be).  We add periodStartDayOfWeek in
		// order to fill out the first week, if it is fractional.
		int weekNo = (dayOfPeriod + periodStartDayOfWeek - 1) / 7;

		// If the first week is long enough, then count it.  If
		// the minimal days in the first week is one, or if the period start
		// is zero, we always increment weekNo.
		if ((7 - periodStartDayOfWeek) >= minDaysInFirstWeek) {
			++weekNo;
		}

		return weekNo;
	}

	// ---------------------------------------------------------------- add/sub time

	/**
	 * Sets date, time is set to midnight (00:00:00.000).
	 *
	 * @param year   year to set
	 * @param month  month to set
	 * @param day    day to set
	 */
	public void set(int year, int month, int day) {
		set(year, month, day, 0, 0, 0, 0);
	}

	/**
	 * Constructor that sets just date. Time is set to 00:00:00.
	 *
	 * @param year   year to set
	 * @param month  month to set
	 * @param day    day to set
	 *
	 * @see #set(int, int, int)
	 */
	public JDateTime(int year, int month, int day) {
		this.set(year, month, day);
	}

	/**
	 * Returns current year.
	 */
	public int getYear() {
		return time.year;
	}

	/**
	 * Returns current month.
	 */
	public int getMonth() {
		return time.month;
	}

	/**
	 * Returns current day of month.
	 */
	public int getDay() {
		return time.day;
	}

	// ----------------------------------------------------------------	other gets
	/**
	 * Creates <code>JDateTime</code> from <code>double</code> that represents JD.
	 */
	public JDateTime(double jd) {
		setJulianDate(new JulianDateStamp(jd));
	}

	// ---------------------------------------------------------------- week definitions

	protected int firstDayOfWeek = JDateTimeDefault.firstDayOfWeek;
	protected int mustHaveDayOfFirstWeek = JDateTimeDefault.mustHaveDayOfFirstWeek;


	// ---------------------------------------------------------------- week definitions (alt)

	protected int minDaysInFirstWeek = JDateTimeDefault.minDaysInFirstWeek;

}
