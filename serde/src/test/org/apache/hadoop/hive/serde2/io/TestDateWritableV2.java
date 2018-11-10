/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.serde2.io;

import com.google.code.tempusfugit.concurrency.ConcurrentRule;
import com.google.code.tempusfugit.concurrency.RepeatingRule;
import com.google.code.tempusfugit.concurrency.annotations.Concurrent;
import com.google.code.tempusfugit.concurrency.annotations.Repeating;
import org.apache.hadoop.hive.common.type.Date;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestDateWritableV2 {
  private static final Logger LOG = LoggerFactory.getLogger(TestDateWritableV2.class);

  @Rule public ConcurrentRule concurrentRule = new ConcurrentRule();
  @Rule public RepeatingRule repeatingRule = new RepeatingRule();

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testConstructor() {
    Date date = Date.valueOf(getRandomDateString());
    DateWritableV2 dw1 = new DateWritableV2(date);
    DateWritableV2 dw2 = new DateWritableV2(dw1);
    DateWritableV2 dw3 = new DateWritableV2(dw1.getDays());

    assertEquals(dw1, dw1);
    assertEquals(dw1, dw2);
    assertEquals(dw2, dw3);
    assertEquals(date, dw1.get());
    assertEquals(date, dw2.get());
    assertEquals(date, dw3.get());
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testComparison() {
    // Get 2 different dates
    Date date1 = Date.valueOf(getRandomDateString());
    Date date2 = Date.valueOf(getRandomDateString());
    while (date1.equals(date2)) {
      date2 = Date.valueOf(getRandomDateString());
    }

    DateWritableV2 dw1 = new DateWritableV2(date1);
    DateWritableV2 dw2 = new DateWritableV2(date2);
    DateWritableV2 dw3 = new DateWritableV2(date1);

    assertTrue("Dates should be equal", dw1.equals(dw1));
    assertTrue("Dates should be equal", dw1.equals(dw3));
    assertTrue("Dates should be equal", dw3.equals(dw1));
    assertEquals("Dates should be equal", 0, dw1.compareTo(dw1));
    assertEquals("Dates should be equal", 0, dw1.compareTo(dw3));
    assertEquals("Dates should be equal", 0, dw3.compareTo(dw1));

    assertFalse("Dates not should be equal", dw1.equals(dw2));
    assertFalse("Dates not should be equal", dw2.equals(dw1));
    assertTrue("Dates not should be equal", 0 != dw1.compareTo(dw2));
    assertTrue("Dates not should be equal", 0 != dw2.compareTo(dw1));
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testGettersSetters() {
    Date date1 = Date.valueOf(getRandomDateString());
    Date date2 = Date.valueOf(getRandomDateString());
    Date date3 = Date.valueOf(getRandomDateString());
    DateWritableV2 dw1 = new DateWritableV2(date1);
    DateWritableV2 dw2 = new DateWritableV2(date2);
    DateWritableV2 dw3 = new DateWritableV2(date3);
    DateWritableV2 dw4 = new DateWritableV2();

    // Getters
    assertEquals(date1, dw1.get());
    assertEquals(date1.toEpochSecond(), dw1.getTimeInSeconds());

    dw4.set(Date.valueOf("1970-01-02"));
    assertEquals(1, dw4.getDays());
    dw4.set(Date.valueOf("1971-01-01"));
    assertEquals(365, dw4.getDays());

    // Setters
    dw4.set(dw1.getDays());
    assertEquals(dw1, dw4);

    dw4.set(dw2.get());
    assertEquals(dw2, dw4);

    dw4.set(dw3);
    assertEquals(dw3, dw4);
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testWritableMethods() throws Throwable {
    DateWritableV2 dw1 = new DateWritableV2(Date.valueOf(getRandomDateString()));
    DateWritableV2 dw2 = new DateWritableV2();
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(byteStream);

    dw1.write(out);
    dw2.readFields(new DataInputStream(new ByteArrayInputStream(byteStream.toByteArray())));

    assertEquals("Dates should be equal", dw1, dw2);
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testDateValueOf() {
    // Just making sure Date.valueOf() works ok
    String dateStr = getRandomDateString();
    Date date = Date.valueOf(dateStr);
    assertEquals(dateStr, date.toString());
  }

  private static String[] dateStrings = new String[365];

  @BeforeClass
  public static void setupDateStrings() {
    Date initialDate = Date.valueOf("2014-01-01");
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(initialDate.toEpochMilli());
    for (int idx = 0; idx < 365; ++idx) {
      dateStrings[idx] = Date.ofEpochMilli(cal.getTimeInMillis()).toString();
      cal.add(1, Calendar.DAY_OF_YEAR);
    }
  }

  private static String getRandomDateString() {
    return dateStrings[(int) (Math.random() * 365)];
  }

  public static class DateTestCallable implements Callable<Void> {
    private LinkedList<DtMismatch> bad;
    private String tz;

    public DateTestCallable(LinkedList<DtMismatch> bad, String tz) {
      this.bad = bad;
      this.tz = tz;
    }

    @Override
    public Void call() throws Exception {
      // Iterate through each day of the year, make sure Date/DateWritableV2 match
      Date originalDate = Date.valueOf("1900-01-01");
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(originalDate.toEpochMilli());
      for (int idx = 0; idx < 365*200; ++idx) {
        originalDate = Date.ofEpochMilli(cal.getTimeInMillis());
        // Make sure originalDate is at midnight in the local time zone,
        // since DateWritableV2 will generate dates at that time.
        originalDate = Date.valueOf(originalDate.toString());
        DateWritableV2 dateWritable = new DateWritableV2(originalDate);
        Date actual = dateWritable.get();
        if (!originalDate.equals(actual)) {
          String originalStr = originalDate.toString();
          String actualStr = actual.toString();
          if (originalStr.substring(0, 10).equals(actualStr.substring(0, 10))) continue;
          bad.add(new DtMismatch(originalStr, actualStr, tz));
        }
        cal.add(Calendar.DAY_OF_YEAR, 1);
      }
      // Success!
      return null;
    }
  }

  private static class DtMismatch {
    String expected, found, tz;
    public DtMismatch(String originalStr, String actualStr, String tz) {
      this.expected = originalStr;
      this.found = actualStr;
      this.tz = tz;
    }
  }

  @Test
  public void testDaylightSavingsTime() throws Exception {
    LinkedList<DtMismatch> bad = new LinkedList<>();

    for (String timeZone: TimeZone.getAvailableIDs()) {
      TimeZone previousDefault = TimeZone.getDefault();
      TimeZone.setDefault(TimeZone.getTimeZone(timeZone));
      assertEquals("Default timezone should now be " + timeZone,
          timeZone, TimeZone.getDefault().getID());
      ExecutorService threadPool = Executors.newFixedThreadPool(1);
      try {
        // TODO: pointless
        threadPool.submit(new DateTestCallable(bad, timeZone)).get();
      } finally {
        threadPool.shutdown(); TimeZone.setDefault(previousDefault);
      }
    }
    StringBuilder errors = new StringBuilder("\nDATE MISMATCH:\n");
    for (DtMismatch dm : bad) {
      errors.append("E ").append(dm.tz).append(": ").append(dm.expected).append(" != ").append(dm.found).append("\n");
    }
    LOG.error(errors.toString());
    if (!bad.isEmpty()) throw new Exception(bad.size() + " mismatches, see logs");
  }
}
