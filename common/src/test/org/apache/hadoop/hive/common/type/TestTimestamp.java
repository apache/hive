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
 * Test suite for {@link Timestamp} class.
 */
public class TestTimestamp {

  @Test
  public void testValueOfSimple() {
    Timestamp t = Timestamp.valueOf("2012-10-10 20:32:12");
    assertEquals("2012-10-10 20:32:12", t.toString());
  }

  @Test
  public void testValueOfOptionalNanosZero() {
    Timestamp t = Timestamp.valueOf("2012-10-10 20:32:12.0");
    assertEquals("2012-10-10 20:32:12", t.toString());
  }

  @Test
  public void testValueOfOptionalNanos() {
    Timestamp t = Timestamp.valueOf("2012-10-10 20:32:12.123");
    assertEquals("2012-10-10 20:32:12.123", t.toString());
  }

  @Test
  public void testValueOfMissingNanos() {
    Timestamp t = Timestamp.valueOf("2012-10-10 20:32:12.");
    assertEquals("2012-10-10 20:32:12", t.toString());
  }

  @Test
  public void testValueOfMissingSeconds() {
    // Seconds are optional - default value is "00"
    Timestamp t = Timestamp.valueOf("2012-10-10 20:32");
    assertEquals("2012-10-10 20:32:00", t.toString());
  }

  @Test
  public void testValueOfMissingMinutes() {
    // Seconds are optional - default value is "00:00"
    Timestamp t = Timestamp.valueOf("2012-10-10 20");
    assertEquals("2012-10-10 20:00:00", t.toString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValueOfIso8601() {
    // Hive does not natively support ISO 8601 format
    // On the table level, alternative timestamp formats can be supported
    // by providing the format to the SerDe property "timestamp.formats"
    Timestamp.valueOf("2012-10-10T20:32:12");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValueOfTooManyNanos() {
    // Can only support up to nine digits of nanos
    Timestamp.valueOf("2012-10-10 20:32:12.0123456789");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValueOfInvalidHour() {
    // Can only support up to nine digits of nanos
    Timestamp.valueOf("2012-10-10 28:32:12");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValueOfInvalidMinute() {
    Timestamp.valueOf("2012-10-10 20:72:12");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValueOfInvalidSecond() {
    Timestamp.valueOf("2012-10-10 20:32:72");
  }

}
