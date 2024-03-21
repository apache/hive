// Copyright (c) 2003-2014, Jodd Team (jodd.org). All Rights Reserved.

package org.apache.hadoop.hive.ql.io.parquet.timestamp.datetime;


import java.io.Serializable;


/**
 * Generic date time stamp just stores and holds date and time information.
 * This class does not contain any date/time logic, neither guarantees
 * that date is valid.
 *
 * @see JDateTime
 * @see JulianDateStamp
 */
public class DateTimeStamp implements Serializable {

	/**
	 * Year.
	 */
	public int year;

	/**
	 * Month, range: [1 - 12]
	 */
	public int month = 1;

	/**
	 * Day, range: [1 - 31]
	 */
	public int day = 1;

	/**
	 * Hour, range: [0 - 23]
	 */
	public int hour;

	/**
	 * Minute, range [0 - 59]
	 */
	public int minute;

	/**
	 * Second, range: [0 - 59]
	 */
	public int second;

	/**
	 * Millisecond, range: [0 - 1000]
	 */
	public int millisecond;
}
