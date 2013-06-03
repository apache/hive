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

package org.apache.hadoop.hive.ql.exec.vector;

import java.sql.Timestamp;

public final class TimestampUtils {

  public static void assignTimeInNanoSec(long timeInNanoSec, Timestamp t) {
    t.setTime((timeInNanoSec)/1000000);
    t.setNanos((int)((t.getNanos()) + (timeInNanoSec % 1000000)));
  }

  public static long getTimeNanoSec(Timestamp t) {
    long time = t.getTime();
    int nanos = t.getNanos();
    return (time * 1000000) + (nanos % 1000000);
  }
}
