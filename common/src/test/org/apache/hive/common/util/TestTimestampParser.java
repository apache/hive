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

package org.apache.hive.common.util;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test suite for parsing timestamps.
 */
public class TestTimestampParser {

  /**
   * No timestamp patterns, should default to normal timestamp format.
   *
   * @see Timestamp#valueOf(String)
   */
  @Test
  public void testDefault() {
    final TimestampParser tsp = new TimestampParser();

    Assert.assertEquals(Timestamp.valueOf("1945-12-31 23:59:59.0"),
        tsp.parseTimestamp("1945-12-31 23:59:59.0"));

    Assert.assertEquals(Timestamp.valueOf("1945-12-31 23:59:59.1234"),
        tsp.parseTimestamp("1945-12-31 23:59:59.1234"));

    Assert.assertEquals(Timestamp.valueOf("1970-01-01 00:00:00"),
        tsp.parseTimestamp("1970-01-01 00:00:00"));

    Assert.assertEquals(Timestamp.valueOf("1945-12-31T23:59:59"),
        tsp.parseTimestamp("1945-12-31 23:59:59"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDefaultInvalid() {
    final TimestampParser tsp = new TimestampParser();
    tsp.parseTimestamp("12345");
  }

  @Test
  public void testPattern1() {
    // Timestamp pattern matching expects fractional seconds length to match
    // the number of 'S' in the pattern. So if you want to match .1, .12, .123,
    // you need 3 different patterns with .S, .SS, .SSS
    // ISO-8601 timestamps
    final String[] patterns = {"yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm:ss.S", "yyyy-MM-dd'T'HH:mm:ss.SS",
        "yyyy-MM-dd'T'HH:mm:ss.SSS", "yyyy-MM-dd'T'HH:mm:ss.SSSS"};

    final TimestampParser tsp = new TimestampParser(patterns);

    Assert.assertEquals(Timestamp.valueOf("1945-12-31 23:59:59.0"),
        tsp.parseTimestamp("1945-12-31T23:59:59.0"));

    Assert.assertEquals(Timestamp.valueOf("2001-01-01 00:00:00.100"),
        tsp.parseTimestamp("2001-01-01T00:00:00.100"));

    Assert.assertEquals(Timestamp.valueOf("2001-01-01 00:00:00.001"),
        tsp.parseTimestamp("2001-01-01T00:00:00.001"));

    Assert.assertEquals(Timestamp.valueOf("1945-12-31T23:59:59.123"),
        tsp.parseTimestamp("1945-12-31T23:59:59.123"));

    Assert.assertEquals(Timestamp.valueOf("1945-12-31T23:59:59.123"),
        tsp.parseTimestamp("1945-12-31T23:59:59.1234"));

    Assert.assertEquals(Timestamp.valueOf("1970-01-01 00:00:00"),
        tsp.parseTimestamp("1970-01-01T00:00:00"));

    /** Default timestamp format still works? */

    Assert.assertEquals(Timestamp.valueOf("1945-12-31 23:59:59.1234"),
        tsp.parseTimestamp("1945-12-31 23:59:59.1234"));

    Assert.assertEquals(Timestamp.valueOf("1945-12-31 23:59:59.12345"),
        tsp.parseTimestamp("1945-12-31T23:59:59.12345"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPatternInvalid1() {
    final String[] patterns = {"yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm:ss.S", "yyyy-MM-dd'T'HH:mm:ss.SS",
        "yyyy-MM-dd'T'HH:mm:ss.SSS", "yyyy-MM-dd'T'HH:mm:ss.SSSS"};

    final TimestampParser tsp = new TimestampParser(patterns);
    tsp.parseTimestamp("1945-12-31-23:59:59");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPatternInvalid2() {
    final String[] patterns = {"yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm:ss.S", "yyyy-MM-dd'T'HH:mm:ss.SS",
        "yyyy-MM-dd'T'HH:mm:ss.SSS", "yyyy-MM-dd'T'HH:mm:ss.SSSS"};

    final TimestampParser tsp = new TimestampParser(patterns);
    tsp.parseTimestamp("12345");
  }

  @Test
  public void testMillisParser() {
    // Also try other patterns
    final String[] patterns = {"millis", "yyyy-MM-dd'T'HH:mm:ss"};

    final TimestampParser tsp = new TimestampParser(patterns);

    Assert.assertEquals(Timestamp.ofEpochMilli(0L), tsp.parseTimestamp("0"));

    Assert.assertEquals(Timestamp.ofEpochMilli(-1000000L),
        tsp.parseTimestamp("-1000000"));

    Assert.assertEquals(Timestamp.ofEpochMilli(1420509274123L),
        tsp.parseTimestamp("1420509274123"));

    Assert.assertEquals(Timestamp.ofEpochMilli(1420509274123L),
        tsp.parseTimestamp("1420509274123.456789"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMillisParserInvalid1() {
    final String[] patterns = {"millis", "yyyy-MM-dd'T'HH:mm:ss"};

    final TimestampParser tsp = new TimestampParser(patterns);
    tsp.parseTimestamp("1420509274123-");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMillisParserInvalid2() {
    // Also try other patterns
    final String[] patterns = {"millis", "yyyy-MM-dd'T'HH:mm:ss"};

    final TimestampParser tsp = new TimestampParser(patterns);
    tsp.parseTimestamp("1945-12-31-23:59:59");
  }

  /**
   * Test for pattern that does not contain all date fields.
   */
  @Test
  public void testPatternShort() {
    final String[] patterns = {"MM:dd:ss", "HH:mm"};

    final TimestampParser tsp = new TimestampParser(patterns);

    Assert.assertEquals(Timestamp.valueOf("1970-01-01 05:06:00"),
        tsp.parseTimestamp("05:06"));

    Assert.assertEquals(Timestamp.valueOf("1970-05-06 00:00:07"),
        tsp.parseTimestamp("05:06:07"));

    Assert.assertEquals(Timestamp.valueOf("1945-12-31 23:59:59"),
        tsp.parseTimestamp("1945-12-31T23:59:59"));
  }

  @Test
  public void testPatternTimeZone() {
    final String[] patterns = {"yyyy-MM-dd'T'HH:mm:ssX"};

    final TimestampParser tsp = new TimestampParser(patterns);
    Assert.assertEquals(Timestamp.valueOf("1945-12-31 23:59:59"),
        tsp.parseTimestamp("1945-12-31T23:59:59Z"));
  }

  @Test
  public void testPatternISO8601() {
    final String[] patterns = {"iso8601"};

    final TimestampParser tsp = new TimestampParser(patterns);
    Assert.assertEquals(Timestamp.valueOf("1945-12-31 23:59:59"),
        tsp.parseTimestamp("1945-12-31T23:59:59Z"));
  }

  @Test
  public void testPatternRFC1123() {
    final String[] patterns = {"rfc1123"};

    final TimestampParser tsp = new TimestampParser(patterns);
    Assert.assertEquals(Timestamp.valueOf("2008-06-03 11:05:30"),
        tsp.parseTimestamp("Tue, 3 Jun 2008 11:05:30 GMT"));
  }
}
