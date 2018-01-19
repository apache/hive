/*
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
package org.apache.hadoop.hive.serde2.io;

import com.google.code.tempusfugit.concurrency.RepeatingRule;
import com.google.code.tempusfugit.concurrency.annotations.Repeating;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;
import org.apache.hadoop.io.WritableComparator;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.time.ZoneId;
import java.util.concurrent.ThreadLocalRandom;

public class TestTimestampTZWritable {

  @Rule
  public RepeatingRule repeatingRule = new RepeatingRule();

  @Test
  @Repeating(repetition = 10)
  public void testSeconds() {
    // just 1 VInt
    long seconds = ThreadLocalRandom.current().nextLong(Integer.MAX_VALUE);
    TimestampTZ tstz = new TimestampTZ(seconds, 0, ZoneId.of("UTC"));
    verifyConversion(tstz);

    // 2 VInt
    seconds = ThreadLocalRandom.current().nextLong(Integer.MAX_VALUE) + Integer.MAX_VALUE + 1;
    if (ThreadLocalRandom.current().nextBoolean()) {
      seconds = -seconds;
    }
    tstz.set(seconds, 0, ZoneId.of("UTC"));
    verifyConversion(tstz);
  }

  @Test
  @Repeating(repetition = 10)
  public void testSecondsWithNanos() {
    long seconds = ThreadLocalRandom.current().nextLong(31556889864403199L);
    if (ThreadLocalRandom.current().nextBoolean()) {
      seconds = -seconds;
    }

    int nanos = ThreadLocalRandom.current().nextInt(999999999) + 1;

    TimestampTZ tstz = new TimestampTZ(seconds, nanos, ZoneId.of("UTC"));
    verifyConversion(tstz);
  }

  @Test
  public void testComparison() {
    String s1 = "2017-04-14 18:00:00 Asia/Shanghai";
    String s2 = "2017-04-14 10:00:00.00 GMT";
    String s3 = "2017-04-14 18:00:00 UTC+08:00";
    String s4 = "2017-04-14 18:00:00 Europe/London";
    TimestampLocalTZWritable writable1 = new TimestampLocalTZWritable(TimestampTZUtil.parse(s1));
    TimestampLocalTZWritable writable2 = new TimestampLocalTZWritable(TimestampTZUtil.parse(s2));
    TimestampLocalTZWritable writable3 = new TimestampLocalTZWritable(TimestampTZUtil.parse(s3));
    TimestampLocalTZWritable writable4 = new TimestampLocalTZWritable(TimestampTZUtil.parse(s4));

    Assert.assertEquals(writable1, writable2);
    Assert.assertEquals(writable1, writable3);
    Assert.assertEquals(writable1.hashCode(), writable2.hashCode());
    Assert.assertEquals(writable1.hashCode(), writable3.hashCode());
    Assert.assertTrue(writable1.compareTo(writable4) < 0);

    byte[] bs1 = writable1.toBinarySortable();
    byte[] bs2 = writable2.toBinarySortable();
    byte[] bs3 = writable3.toBinarySortable();
    byte[] bs4 = writable4.toBinarySortable();
    Assert.assertTrue(WritableComparator.compareBytes(bs1, 0, bs1.length, bs2, 0, bs2.length) == 0);
    Assert.assertTrue(WritableComparator.compareBytes(bs1, 0, bs1.length, bs3, 0, bs3.length) == 0);
    Assert.assertTrue(WritableComparator.compareBytes(bs1, 0, bs1.length, bs4, 0, bs4.length) < 0);
  }

  private static void verifyConversion(TimestampTZ srcTstz) {
    TimestampLocalTZWritable src = new TimestampLocalTZWritable(srcTstz);
    byte[] bytes = src.getBytes();
    TimestampLocalTZWritable dest = new TimestampLocalTZWritable(bytes, 0, ZoneId.of("UTC"));
    TimestampTZ destTstz = dest.getTimestampTZ();
    String errMsg = "Src tstz with seconds " + srcTstz.getEpochSecond() + ", nanos " +
        srcTstz.getNanos() + ". Dest tstz with seconds " + destTstz.getEpochSecond() +
        ", nanos " + destTstz.getNanos();
    Assert.assertEquals(errMsg, srcTstz, destTstz);
  }
}
