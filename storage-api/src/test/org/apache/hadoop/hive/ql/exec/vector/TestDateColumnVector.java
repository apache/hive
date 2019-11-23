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
package org.apache.hadoop.hive.ql.exec.vector;

import org.junit.Assert;
import org.junit.Test;

public class TestDateColumnVector {
  /**
   * Test case for DateColumnVector's changeCalendar
   * epoch days, hybrid representation, proleptic representation
   *   16768: hybrid: 2015-11-29 proleptic: 2015-11-29
   * -141418: hybrid: 1582-10-24 proleptic: 1582-10-24
   * -141427: hybrid: 1582-10-15 proleptic: 1582-10-15
   * -141428: hybrid: 1582-10-04 proleptic: 1582-10-14
   * -141430: hybrid: 1582-10-02 proleptic: 1582-10-12
   * -141437: hybrid: 1582-09-25 proleptic: 1582-10-05
   * -141438: hybrid: 1582-09-24 proleptic: 1582-10-04
   * -499952: hybrid: 0601-03-04 proleptic: 0601-03-07
   * -499955: hybrid: 0601-03-01 proleptic: 0601-03-04
   * @throws Exception 
   */
  @Test
  public void testProlepticCalendar() throws Exception {
    // the expected output is the same as it was with the original calendar,
    // as the goal is to change the underlying data in a way that the user
    // gets the same string representation after the calendar change

    // from hybrid internal representation to proleptic
    setDateAndVerifyProlepticUpdate(16768, "2015-11-29", false, true);
    setDateAndVerifyProlepticUpdate(-141418, "1582-10-24", false, true);
    setDateAndVerifyProlepticUpdate(-141427, "1582-10-15", false, true);
    setDateAndVerifyProlepticUpdate(-141428, "1582-10-04", false, true);
    setDateAndVerifyProlepticUpdate(-141430, "1582-10-02", false, true);
    setDateAndVerifyProlepticUpdate(-141437, "1582-09-25", false, true);
    setDateAndVerifyProlepticUpdate(-499952, "0601-03-04", false, true);
    setDateAndVerifyProlepticUpdate(-499955, "0601-03-01", false, true);

    // from proleptic internal representation to hybrid
    // this way, some string representations will change, as some proleptic dates
    // (represented as string) don't exist in hybrid calendar, e.g. '1582-10-14'
    setDateAndVerifyProlepticUpdate(16768, "2015-11-29", true, false);
    setDateAndVerifyProlepticUpdate(-141418, "1582-10-24", true, false);
    setDateAndVerifyProlepticUpdate(-141427, "1582-10-15", true, false);
    setDateAndVerifyProlepticUpdate(-141428, "1582-10-24", true, false); // 1582-10-14 -> 1582-10-24
    setDateAndVerifyProlepticUpdate(-141430, "1582-10-22", true, false); // 1582-10-12 -> 1582-10-22
    setDateAndVerifyProlepticUpdate(-141437, "1582-10-15", true, false); // 1582-10-05 -> 1582-10-15
    setDateAndVerifyProlepticUpdate(-499952, "0601-03-07", true, false);
    setDateAndVerifyProlepticUpdate(-499955, "0601-03-04", true, false);
  }

  private void setDateAndVerifyProlepticUpdate(long longDay, String expectedDateString,
      boolean originalUseProleptic, boolean newUseProleptic) throws Exception {

    DateColumnVector dateColumnVector =
        new DateColumnVector().setUsingProlepticCalendar(originalUseProleptic);
    dateColumnVector.vector[0] = longDay;

    dateColumnVector.changeCalendar(newUseProleptic, true);

    Assert.assertEquals("original = " + originalUseProleptic +
                        " new = " + newUseProleptic,
                        expectedDateString, dateColumnVector.formatDate(0));
  }
}
