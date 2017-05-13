/**
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

package org.apache.hadoop.hive.common.type;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

public class TestTimestampTZ {
  @Test
  public void testConvertToUTC() {
    String s = "2017-04-14 18:00:00 Asia/Shanghai";
    TimestampTZ timestampTZ = TimestampTZ.parse(s);
    Assert.assertEquals("2017-04-14 10:00:00.0 Z", timestampTZ.toString());
  }

  @Test
  public void testComparison() {
    String s1 = "2017-04-14 18:00:00 Asia/Shanghai";
    String s2 = "2017-04-14 10:00:00.00 GMT";
    String s3 = "2017-04-14 18:00:00 UTC+08:00";
    String s4 = "2017-04-14 18:00:00 Europe/London";
    TimestampTZ tstz1 = TimestampTZ.parse(s1);
    TimestampTZ tstz2 = TimestampTZ.parse(s2);
    TimestampTZ tstz3 = TimestampTZ.parse(s3);
    TimestampTZ tstz4 = TimestampTZ.parse(s4);

    Assert.assertEquals(tstz1, tstz2);
    Assert.assertEquals(tstz1, tstz3);
    Assert.assertEquals(tstz1.hashCode(), tstz2.hashCode());
    Assert.assertEquals(tstz1.hashCode(), tstz3.hashCode());
    Assert.assertTrue(tstz1.compareTo(tstz4) < 0);
  }

  @Test
  public void testDST() {
    String s1 = "2005-04-03 02:01:00 America/Los_Angeles";
    String s2 = "2005-04-03 03:01:00 America/Los_Angeles";
    Assert.assertEquals(TimestampTZ.parse(s1), TimestampTZ.parse(s2));
  }

  @Test
  public void testFromToInstant() {
    String s1 = "2017-04-14 18:00:00 UTC";
    TimestampTZ tstz = TimestampTZ.parse(s1);
    long seconds = tstz.getEpochSecond();
    int nanos = tstz.getNanos();
    Assert.assertEquals(tstz, new TimestampTZ(seconds, nanos));

    nanos += 123000000;
    Assert.assertEquals("2017-04-14 18:00:00.123 Z", new TimestampTZ(seconds, nanos).toString());

    seconds -= 3;
    Assert.assertEquals("2017-04-14 17:59:57.123 Z", new TimestampTZ(seconds, nanos).toString());
  }

  @Test
  public void testVariations() {
    // Omitting zone or time part is allowed
    TimestampTZ.parse("2017-01-01 13:33:00");
    TimestampTZ.parse("2017-11-08 Europe/London");
    TimestampTZ.parse("2017-05-20");
    TimestampTZ.parse("2017-11-08GMT");
    TimestampTZ.parse("2017-10-11 GMT+8:00");
    TimestampTZ.parse("2017-05-08 07:45:00-3:00");
  }

  @Test
  public void testInvalidStrings() {
    // invalid zone
    try {
      TimestampTZ.parse("2017-01-01 13:33:00 foo");
      Assert.fail("Invalid timezone ID should cause exception");
    } catch (DateTimeParseException e) {
      // expected
    }
    // invalid time part
    try {
      TimestampTZ.parse("2017-01-01 13:33:61");
      Assert.fail("Invalid time should cause exception");
    } catch (DateTimeParseException e) {
      // expected
    }
  }

  @Test
  public void testConvertFromTimestamp() {
    TimeZone defaultZone = TimeZone.getDefault();
    try {
      // Use system zone when converting from timestamp to timestamptz
      String s = "2017-06-12 23:12:56.34";
      TimeZone.setDefault(TimeZone.getTimeZone("Europe/London"));
      TimestampTZ tstz1 = TimestampTZ.convert(Timestamp.valueOf(s));
      TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
      TimestampTZ tstz2 = TimestampTZ.convert(Timestamp.valueOf(s));
      Assert.assertTrue(tstz1.compareTo(tstz2) < 0);
    } finally {
      TimeZone.setDefault(defaultZone);
    }
  }
}
