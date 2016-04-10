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

import java.sql.Timestamp;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class RandomTypeUtil {

  public static final long NANOSECONDS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

  public static Timestamp getRandTimestamp(Random r) {
    String optionalNanos = "";
    if (r.nextInt(2) == 1) {
      optionalNanos = String.format(".%09d",
          Integer.valueOf(0 + r.nextInt((int) NANOSECONDS_PER_SECOND)));
    }
    String timestampStr = String.format("%04d-%02d-%02d %02d:%02d:%02d%s",
        Integer.valueOf(0 + r.nextInt(10000)),  // year
        Integer.valueOf(1 + r.nextInt(12)),      // month
        Integer.valueOf(1 + r.nextInt(28)),      // day
        Integer.valueOf(0 + r.nextInt(24)),      // hour
        Integer.valueOf(0 + r.nextInt(60)),      // minute
        Integer.valueOf(0 + r.nextInt(60)),      // second
        optionalNanos);
    Timestamp timestampVal;
    try {
      timestampVal = Timestamp.valueOf(timestampStr);
    } catch (Exception e) {
      System.err.println("Timestamp string " + timestampStr + " did not parse");
      throw e;
    }
    return timestampVal;
  }
}