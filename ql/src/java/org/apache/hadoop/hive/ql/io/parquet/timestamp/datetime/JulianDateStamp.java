// Copyright (c) 2003-2014, Jodd Team (jodd.org). All Rights Reserved.

package org.apache.hadoop.hive.ql.io.parquet.timestamp.datetime;

import java.io.Serializable;

/**
 * Julian Date stamp, for high precision calculations. Julian date is a real
 * number and it basically consist of two parts: integer and fraction. Integer
 * part carries date information, fraction carries time information.
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
 * <p>
 * For calculations that will have time precision of 1e-3 seconds, both
 * fraction and integer part must have enough digits in it. The problem is
 * that integer part is big and, on the other hand fractional is small, and
 * since final julian date is a sum of this two values, some fraction
 * numerals may be lost. Therefore, for higher precision both
 * fractional and integer part of julian date real number has to be
 * preserved.
 * <p>
 * This class stores the unmodified fraction part, but not all digits
 * are significant! For 1e-3 seconds precision, only 8 digits after
 * the decimal point are significant.
 *
 * @see TimeUtil
 * @see JDateTime
 * @see DateTimeStamp
 */
public class JulianDateStamp implements Serializable, Cloneable {

	/**
	 * Integer part of the Julian Date (JD).
	 */
	protected int integer;

	/**
	 * Fraction part of the Julian Date (JD).
	 * Should be always in [0.0, 1.0) range.
	 */
	protected double fraction;

	/**
	 * Returns JDN. Note that JDN is not equal to {@link #integer}. It is calculated by
	 * rounding to the nearest integer.
	 */
	public int getJulianDayNumber() {
		if (fraction >= 0.5) {
			return integer + 1;
		}
		return integer;
	}

	// ---------------------------------------------------------------- ctors

	/**
	 * Creates JD from a <code>double</code>.
	 */
	public JulianDateStamp(double jd) {
		set(jd);
	}

	/**
	 * Creates JD from both integer and fractional part using normalization.
	 * Normalization occurs when fractional part is out of range. 
	 *
	 * @see #set(int, double)
	 *
	 * @param i      integer part
	 * @param f      fractional part should be in range [0.0, 1.0)
	 */
	public JulianDateStamp(int i, double f) {
		set(i, f);
	}

	// ---------------------------------------------------------------- conversion
	

	/**
	 * Returns <code>double</code> value of JD.
	 * <b>CAUTION</b>: double values may not be suit for precision math due to
	 * loss of precision.
	 */
	public double doubleValue() {
		return (double)integer + fraction;
	}

	/**
	 * Returns string representation of JD.
	 *
	 * @return julian integer as string
	 */
	@Override
	public String toString() {
		String s = Double.toString(fraction);
		int i = s.indexOf('.');
		s = s.substring(i);
		return integer + s;
	}


	// ---------------------------------------------------------------- math


	/**
	 * Sets integer and fractional part with normalization.
	 * Normalization means that if double is out of range,
	 * values will be correctly fixed. 
	 */
	public void set(int i, double f) {
		integer = i;
		int fi = (int) f;
		f -= fi;
		integer += fi;
		if (f < 0) {
			f += 1;
			integer--;
		}
		this.fraction = f;
	}

	public void set(double jd) {
		integer = (int)jd;
		fraction = jd - (double)integer;
	}

	@Override
	protected JulianDateStamp clone() {
		return new JulianDateStamp(this.integer, this.fraction);
	}
}
