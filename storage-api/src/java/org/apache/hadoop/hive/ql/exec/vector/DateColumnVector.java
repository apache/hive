/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.vector;

import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * This class extends LongColumnVector in order to introduce some date-specific semantics. In
 * DateColumnVector, the elements of vector[] represent the days since 1970-01-01
 */
public class DateColumnVector extends LongColumnVector {
  private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
  private static final GregorianCalendar PROLEPTIC_GREGORIAN_CALENDAR = new GregorianCalendar(UTC);
  private static final GregorianCalendar GREGORIAN_CALENDAR = new GregorianCalendar(UTC);

  private static final SimpleDateFormat PROLEPTIC_GREGORIAN_DATE_FORMATTER =
      new SimpleDateFormat("yyyy-MM-dd");
  private static final SimpleDateFormat GREGORIAN_DATE_FORMATTER =
      new SimpleDateFormat("yyyy-MM-dd");

  /**
  * -141427: hybrid: 1582-10-15 proleptic: 1582-10-15
  * -141428: hybrid: 1582-10-04 proleptic: 1582-10-14
  */
  private static final int CUTOVER_DAY_EPOCH = -141427; // it's 1582-10-15 in both calendars

  static {
    PROLEPTIC_GREGORIAN_CALENDAR.setGregorianChange(new java.util.Date(Long.MIN_VALUE));

    PROLEPTIC_GREGORIAN_DATE_FORMATTER.setCalendar(PROLEPTIC_GREGORIAN_CALENDAR);
    GREGORIAN_DATE_FORMATTER.setCalendar(GREGORIAN_CALENDAR);
  }

  private boolean usingProlepticCalendar = false;

  public DateColumnVector() {
    this(VectorizedRowBatch.DEFAULT_SIZE);
  }

  /**
   * Change the calendar to or from proleptic. If the new and old values of the flag are the same,
   * nothing is done. useProleptic - set the flag for the proleptic calendar updateData - change the
   * data to match the new value of the flag.
   */
  public void changeCalendar(boolean useProleptic, boolean updateData) {
    if (useProleptic == usingProlepticCalendar) {
      return;
    }
    usingProlepticCalendar = useProleptic;
    if (updateData) {
      try {
        updateDataAccordingProlepticSetting();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void updateDataAccordingProlepticSetting() throws Exception {
    for (int i = 0; i < vector.length; i++) {
      if (vector[i] >= CUTOVER_DAY_EPOCH) { // no need for conversion
        continue;
      }
      long millis = TimeUnit.DAYS.toMillis(vector[i]);
      String originalFormatted = usingProlepticCalendar ? GREGORIAN_DATE_FORMATTER.format(millis)
        : PROLEPTIC_GREGORIAN_DATE_FORMATTER.format(millis);

      millis = (usingProlepticCalendar ? PROLEPTIC_GREGORIAN_DATE_FORMATTER.parse(originalFormatted)
        : GREGORIAN_DATE_FORMATTER.parse(originalFormatted)).getTime();

      vector[i] = TimeUnit.MILLISECONDS.toDays(millis);
    }
  }

  public String formatDate(int i) {
    long millis = TimeUnit.DAYS.toMillis(vector[i]);
    return usingProlepticCalendar ? PROLEPTIC_GREGORIAN_DATE_FORMATTER.format(millis)
      : GREGORIAN_DATE_FORMATTER.format(millis);
  }

  public DateColumnVector setUsingProlepticCalendar(boolean usingProlepticCalendar) {
    this.usingProlepticCalendar = usingProlepticCalendar;
    return this;
  }

  /**
   * Detect whether this data is using the proleptic calendar.
   */
  public boolean isUsingProlepticCalendar() {
    return usingProlepticCalendar;
  }

  /**
   * Don't use this except for testing purposes.
   *
   * @param len the number of rows
   */
  public DateColumnVector(int len) {
    super(len);
  }

  @Override
  public void shallowCopyTo(ColumnVector otherCv) {
    DateColumnVector other = (DateColumnVector) otherCv;
    super.shallowCopyTo(other);
    other.vector = vector;
  }
}
