// Copyright (c) 2003-2014, Jodd Team (jodd.org). All Rights Reserved.

package org.apache.hadoop.hive.ql.io.parquet.timestamp.datetime;


/**
 * Date time calculations and utilities. <code>TimeUtil</code> is used by
 * {@link JDateTime} and it contains few utilities that may be used
 * elsewhere, although {@link JDateTime} is recommended for all time
 * manipulation.
 */
public class TimeUtil {
	// ---------------------------------------------------------------- misc calc

	/**
	 * Check if the given year is leap year.
	 *
	 * @return <code>true</code> if the year is a leap year
	 */
	public static boolean isLeapYear(int y) {
		boolean result = false;
	
		if (((y % 4) == 0) &&			// must be divisible by 4...
			((y < 1582) ||				// and either before reform year...
			 ((y % 100) != 0) ||		// or not a century...
			 ((y % 400) == 0))) {		// or a multiple of 400...
				result = true;			// for leap year.
		}
		return result;
	}

	private static final int[] MONTH_LENGTH = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
	private static final int[] LEAP_MONTH_LENGTH = {0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

	/**
	 * Returns the length of the specified month in days. Month is 1 for January
	 * and 12 for December. It works only for years after 1582.
	 *
	 * @return length of the specified month in days
	 */
	public static int getMonthLength(int year, int m) {
		if ((m < 1) || (m > 12)) {
			return -1;
		}
		if (isLeapYear(year)) {
			return LEAP_MONTH_LENGTH[m];
		}
		return MONTH_LENGTH[m];
	}


	// ---------------------------------------------------------------- valid

	/**
	 * Checks if date is valid.
	 *
	 * @return <code>true</code> if date is valid, otherwise <code>false</code>
	 */
	public static boolean isValidDate(int year, int month, int day) {
		if ((month < 1) || (month > 12)) {
			return false;
		}
		int ml = getMonthLength(year, month);
		//noinspection RedundantIfStatement
		if ((day < 1) || (day > ml)) {
			return false;
		}
		return true;
	}

	/**
	 * Checks if time is valid.
	 *
	 * @param hour   hour to check
	 * @param minute minute to check
	 * @param second second to check
	 *
	 * @return <code>true</code> if time is valid, otherwise <code>false</code>
	 */
	public static boolean isValidTime(int hour, int minute, int second, int millisecond) {
		if ((hour < 0) || (hour >= 24)) {
			return false;
		}
		if ((minute < 0) || (minute >= 60)) {
			return false;
		}
		if ((second < 0) || (second >= 60)) {
			return false;
		}
		//noinspection RedundantIfStatement
		if ((millisecond < 0) || (millisecond >= 1000)) {
			return false;
		}
		return true;
	}

	/**
	 * Checks if date and time are valid.
	 *
	 * @param year   year to check
	 * @param month  month to check
	 * @param day    day to check
	 * @param hour   hour to check
	 * @param minute minute to check
	 * @param second second to check
	 *
	 * @return <code>true</code> if date and time are valid, otherwise <code>false</code>
	 */
	public static boolean isValidDateTime(int year, int month, int day, int hour, int minute, int second, int millisecond) {
		return (isValidDate(year, month, day) && isValidTime(hour, minute, second, millisecond));
	}


	/**
	 * Calculates Astronomical Julian Date from given time.<p>
	 *
	 * Astronomical Julian Dates are counting from noon of the January 1st, -4712
	 * (julian date 0 is -4712/01/01 12:00:00). Zero year exist. Julian Date
	 * is always GMT, there are no timezones.
	 * <p>
	 *
	 * Algorithm based on Numerical Recipesin C, 2nd ed., Cambridge University
	 * Press 1992, modified and enhanced by Igor Spasic.
	 *
	 * @param year   year
	 * @param month  month
	 * @param day    day
	 * @param hour   hour
	 * @param minute minute
	 * @param second second
	 *
	 * @return julian time stamp
	 */
	public static JulianDateStamp toJulianDate(int year, int month, int day, int hour, int minute, int second, int millisecond) {

		// month range fix
		if ((month > 12) || (month < -12)) {
			month--;
			int delta = month / 12;
			year += delta;
			month -= delta * 12;
			month++;
		}
		if (month < 0) {
			year--;
			month += 12;
		}

		// decimal day fraction	
		double frac = (hour / 24.0) + (minute / 1440.0) + (second / 86400.0) + (millisecond / 86400000.0);
		if (frac < 0) {		// negative time fix
			int delta = ((int)(-frac)) + 1;
			frac += delta;
			day -= delta;
		}
		//double gyr = year + (0.01 * month) + (0.0001 * day) + (0.0001 * frac) + 1.0e-9;
		double gyr = year + (0.01 * month) + (0.0001 * (day + frac)) + 1.0e-9;

		// conversion factors
		int iy0;
		int im0;
		if (month <= 2) {
			iy0 = year - 1;
			im0 = month + 12;
		} else {
			iy0 = year;
			im0 = month;
		}
		int ia = iy0 / 100;
		int ib = 2 - ia + (ia >> 2);
	
		// calculate julian date
		int jd;
		if (year <= 0) {
			jd = (int)((365.25 * iy0) - 0.75) + (int)(30.6001 * (im0 + 1)) + day + 1720994;
		} else {
			jd = (int)(365.25 * iy0) + (int)(30.6001 * (im0 + 1)) + day + 1720994;
		}
		if (gyr >= 1582.1015) {						// on or after 15 October 1582
			jd += ib;
		}
		//return  jd + frac + 0.5;

		return new JulianDateStamp(jd, frac + 0.5);
	}


	/**
	 * Calculates time stamp from Astronomical Julian Date.
	 * Algorithm based on Numerical Recipesin C, 2nd ed., Cambridge University
	 * Press 1992.
	 *
	 * @param jds    julian date stamp
	 *
	 * @return time stamp
	 */
	public static DateTimeStamp fromJulianDate(JulianDateStamp jds) {
		DateTimeStamp time = new DateTimeStamp();
		int year, month, day;
		double frac;
		int jd, ka, kb, kc, kd, ke, ialp;
		
		//double JD = jds.doubleValue();//jdate;
		//jd = (int)(JD + 0.5);							// integer julian date
		//frac = JD + 0.5 - (double)jd + 1.0e-10;		// day fraction

		ka = (int)(jds.fraction + 0.5);
		jd = jds.integer + ka;
		frac = jds.fraction + 0.5 - ka + 1.0e-10;

		ka = jd;
		if (jd >= 2299161) {
			ialp = (int)(((double)jd - 1867216.25) / (36524.25));
			ka = jd + 1 + ialp - (ialp >> 2);
		}
		kb = ka + 1524;
		kc =  (int)(((double)kb - 122.1) / 365.25);
		kd = (int)((double)kc * 365.25);
		ke = (int)((double)(kb - kd) / 30.6001);
		day = kb - kd - ((int)((double)ke * 30.6001));
		if (ke > 13) {
			month = ke - 13;
		} else {
			month = ke - 1;
		}
		if ((month == 2) && (day > 28)){
			day = 29;
		}
		if ((month == 2) && (day == 29) && (ke == 3)) {
			year = kc - 4716;
		} else if (month > 2) {
			year = kc - 4716;
		} else {
			year = kc - 4715;
		}
		time.year = year;
		time.month = month;
		time.day = day;

		// hour with minute and second included as fraction
		double d_hour = frac * 24.0;
		time.hour = (int) d_hour;				// integer hour

		// minute with second included as a fraction
		double d_minute = (d_hour - (double)time.hour) * 60.0;
		time.minute = (int) d_minute;			// integer minute

		double d_second = (d_minute - (double)time.minute) * 60.0;
		time.second = (int) d_second;			// integer seconds 

		double d_millis = (d_second - (double)time.second) * 1000.0;

		// fix calculation errors
		time.millisecond = (int) (((d_millis * 10) + 0.5) / 10);

		return time;
	}
}
