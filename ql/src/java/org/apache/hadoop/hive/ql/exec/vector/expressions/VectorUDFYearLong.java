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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.util.Arrays;
import java.util.Calendar;


public final class VectorUDFYearLong extends VectorUDFTimestampFieldLong {

  /* year boundaries in nanoseconds */
  static final long[] yearBoundaries;
  static final int minYear = 1901;
  static final int maxYear = 2038;

  static {
    yearBoundaries = new long[maxYear-minYear];
    Calendar c = Calendar.getInstance();
    c.setTimeInMillis(0); // c.set doesn't reset millis
    /* 1901 Jan is not with in range */
    for(int year=minYear+1; year <= 2038; year++) {
      c.set(year, Calendar.JANUARY, 1, 0, 0, 0);
      yearBoundaries[year-minYear-1] = c.getTimeInMillis()*1000*1000;
    }
  }

  @Override
  protected long getField(long time) {
    /* binarySearch is faster than a loop doing a[i] (no array out of bounds checks) */
    int year = Arrays.binarySearch(yearBoundaries, time);
    if(year >= 0) {
      /* 0 == 1902 etc */
      return minYear + 1 + year;
    } else {
      /* -1 == 1901, -2 == 1902 */
      return minYear - 1 - year;
    }
  }

  public VectorUDFYearLong(int colNum, int outputColumn) {
    super(Calendar.YEAR, colNum,outputColumn);
  }

}
