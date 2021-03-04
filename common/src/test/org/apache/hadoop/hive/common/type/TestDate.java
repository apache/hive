/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.common.type;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Test suite for {@link Date} class.
 */
public class TestDate {

  @Test
  public void testValueOfSimple() {
    Date d = Date.valueOf("2012-02-21");
    assertEquals("2012-02-21", d.toString());
  }

  @Test
  public void testValueOfMinValue() {
    Date d = Date.valueOf("0000-01-01");
    assertEquals("0000-01-01", d.toString());
  }

  @Test
  public void testValueOfMaxValue() {
    Date d = Date.valueOf("9999-12-31");
    assertEquals("9999-12-31", d.toString());
  }

  @Test
  public void testValueOfPrePad() {
    Date d = Date.valueOf(" 2012-02-21");
    assertEquals("2012-02-21", d.toString());
  }

  @Test
  public void testValueOfPostPad() {
    Date d = Date.valueOf("2012-02-21 ");
    assertEquals("2012-02-21", d.toString());
  }

  @Test
  public void testValueOfPad() {
    Date d = Date.valueOf(" 2012-02-21 ");
    assertEquals("2012-02-21", d.toString());
  }

  @Test
  public void testValueOfOneDigitYear() {
    Date d = Date.valueOf("2-02-21");
    assertEquals("0002-02-21", d.toString());
  }

  @Test
  public void testValueOfTwoDigitYear() {
    Date d = Date.valueOf("20-02-21");
    assertEquals("0020-02-21", d.toString());
  }

  @Test
  public void testValueOfThreeDigitYear() {
    Date d = Date.valueOf("210-02-21");
    assertEquals("0210-02-21", d.toString());
  }

  @Test
  public void testValueOfOneDigitMonth() {
    Date d = Date.valueOf("2010-2-21");
    assertEquals("2010-02-21", d.toString());
  }

  @Test
  public void testValueOfOneDigitDay() {
    Date d = Date.valueOf("2010-02-2");
    assertEquals("2010-02-02", d.toString());
  }

  @Test
  public void testValueOfOneDigitDayOneDigitMonth() {
    Date d = Date.valueOf("2010-2-2");
    assertEquals("2010-02-02", d.toString());
  }

  @Test
  public void testValueOfLeapDay() {
    // Year 2020 has a leap day
    Date.valueOf("2020-02-29");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValueOfInvalidFormat() {
    // Common, but invalid, date format mm-dd-yyyy
    Date.valueOf("12-01-2018");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValueOfInvalidMonth() {
    // There is no "13" month
    Date.valueOf("2001-13-12");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValueOfInvalidDay() {
    // There is no "32" day of month
    Date.valueOf("2001-12-32");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValueOfInvalidLeapDay() {
    // Year 2019 was not a leap year so there is no "29" day of month
    Date.valueOf("2019-02-29");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValueOfInvalidISO8601() {
    // Date format is yyyy-mm-dd, it does not accept full timestamp strings
    Date.valueOf("2019-02-29T12:23:21.123");
  }

}
