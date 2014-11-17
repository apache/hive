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

import com.google.code.tempusfugit.concurrency.annotations.*;
import com.google.code.tempusfugit.concurrency.*;
import org.junit.*;

import static org.junit.Assert.*;
import java.io.*;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TestDateWritable {

  @Rule public ConcurrentRule concurrentRule = new ConcurrentRule();
  @Rule public RepeatingRule repeatingRule = new RepeatingRule();

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testConstructor() {
    Date date = Date.valueOf(getRandomDateString());
    DateWritable dw1 = new DateWritable(date);
    DateWritable dw2 = new DateWritable(dw1);
    DateWritable dw3 = new DateWritable(dw1.getDays());

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

    DateWritable dw1 = new DateWritable(date1);
    DateWritable dw2 = new DateWritable(date2);
    DateWritable dw3 = new DateWritable(date1);

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
    DateWritable dw1 = new DateWritable(date1);
    DateWritable dw2 = new DateWritable(date2);
    DateWritable dw3 = new DateWritable(date3);
    DateWritable dw4 = new DateWritable();

    // Getters
    assertEquals(date1, dw1.get());
    assertEquals(date1.getTime() / 1000, dw1.getTimeInSeconds());

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
    DateWritable dw1 = new DateWritable(Date.valueOf(getRandomDateString()));
    DateWritable dw2 = new DateWritable();
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
    DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    Date initialDate = Date.valueOf("2014-01-01");
    Calendar cal = Calendar.getInstance();
    cal.setTime(initialDate);
    for (int idx = 0; idx < 365; ++idx) {
      dateStrings[idx] = format.format(cal.getTime());
      cal.add(1, Calendar.DAY_OF_YEAR);
    }
  }

  private static String getRandomDateString() {
    return dateStrings[(int) (Math.random() * 365)];
  }

  public static class DateTestCallable implements Callable<String> {
    public DateTestCallable() {
    }

    @Override
    public String call() throws Exception {
      // Iterate through each day of the year, make sure Date/DateWritable match
      Date originalDate = Date.valueOf("2014-01-01");
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(originalDate.getTime());
      for (int idx = 0; idx < 365; ++idx) {
        originalDate = new Date(cal.getTimeInMillis());
        // Make sure originalDate is at midnight in the local time zone,
        // since DateWritable will generate dates at that time.
        originalDate = Date.valueOf(originalDate.toString());
        DateWritable dateWritable = new DateWritable(originalDate);
        if (!originalDate.equals(dateWritable.get())) {
          return originalDate.toString();
        }
        cal.add(Calendar.DAY_OF_YEAR, 1);
      }
      // Success!
      return null;
    }
  }

  @Test
  public void testDaylightSavingsTime() throws InterruptedException, ExecutionException {
    String[] timeZones = {
        "GMT",
        "UTC",
        "America/Godthab",
        "America/Los_Angeles",
        "Asia/Jerusalem",
        "Australia/Melbourne",
        "Europe/London",
        // time zones with half hour boundaries
        "America/St_Johns",
        "Asia/Tehran",
    };

    for (String timeZone: timeZones) {
      TimeZone previousDefault = TimeZone.getDefault();
      TimeZone.setDefault(TimeZone.getTimeZone(timeZone));
      assertEquals("Default timezone should now be " + timeZone,
          timeZone, TimeZone.getDefault().getID());
      ExecutorService threadPool = Executors.newFixedThreadPool(1);
      try {
        Future<String> future = threadPool.submit(new DateTestCallable());
        String result = future.get();
        assertNull("Failed at timezone " + timeZone + ", date " + result, result);
      } finally {
        threadPool.shutdown(); TimeZone.setDefault(previousDefault);
      }
    }
  }
}
