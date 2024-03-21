// Copyright (c) 2003-2014, Jodd Team (jodd.org). All Rights Reserved.

package org.apache.hadoop.hive.ql.io.parquet.timestamp.datetime;

/**
 * Defaults for {@link JDateTime}.
 */
public class JDateTimeDefault {

	/**
	 * Default definition of first day of week.
	 */
	public static int firstDayOfWeek = JDateTime.MONDAY;

	/**
	 * Default number of days first week of year must have.
	 */
	public static int mustHaveDayOfFirstWeek = 4;

	/**
	 * Default minimal number of days firs week of year must have.
	 */
	public static int minDaysInFirstWeek = 4;
}
