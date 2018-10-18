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

package org.apache.hadoop.hive.common.io;

import org.junit.Test;

public class TestDiskRangeList {
  @Test
  public void testErrorConditions() throws Exception {
    DiskRangeList d510 = new DiskRangeList(0, 10);
    d510.insertPartBefore(new DiskRangeList(0, 5));
    DiskRangeList d1015 = d510.insertAfter(new DiskRangeList(10, 15));
    try {
      d510.replaceSelfWith(d510); // The arg is self.
      fail();
    } catch (AssertionError error) {}
    DiskRangeList existing = new DiskRangeList(0, 10);
    existing.insertPartBefore(new DiskRangeList(0, 5));
    try {
      d510.replaceSelfWith(existing); // The arg is part of another list.
      fail();
    } catch (AssertionError error) {}
    try {
      d510.replaceSelfWith(new DiskRangeList(4, 10)); // Not sequential with previous.
      fail();
    } catch (AssertionError error) {}
    try {
      d510.replaceSelfWith(new DiskRangeList(5, 11)); // Not sequential with next.
      fail();
    } catch (AssertionError error) {}


    try {
      d510.insertPartBefore(d510); // The arg is self.
      fail();
    } catch (AssertionError error) {}
    existing = new DiskRangeList(5, 7);
    existing.insertPartBefore(new DiskRangeList(5, 6));
    try {
      d510.insertPartBefore(existing); // The arg is part of another list.
      fail();
    } catch (AssertionError error) {}
    try {
      d510.insertPartBefore(new DiskRangeList(3, 4)); // Not a part.
      fail();
    } catch (AssertionError error) {}
    try {
      d510.insertPartBefore(new DiskRangeList(4, 6)); // Not sequential with previous.
      fail();
    } catch (AssertionError error) {}


    try {
      d510.insertAfter(d510); // The arg is self.
      fail();
    } catch (AssertionError error) {}
    existing = new DiskRangeList(15, 20);
    existing.insertAfter(new DiskRangeList(20, 25));
    try {
      d1015.insertAfter(existing); // The arg is part of another list.
      fail();
    } catch (AssertionError error) {}
    try {
      d1015.insertAfter(new DiskRangeList(14, 20)); // Not sequential.
      fail();
    } catch (AssertionError error) {}
    d1015.insertAfter(new DiskRangeList(20, 25));
    try {
      d1015.insertAfter(new DiskRangeList(15, 21)); // Not sequential with next.
      fail();
    } catch (AssertionError error) {}
    try {
      d1015.insertPartAfter(new DiskRangeList(16, 20)); // Not a part.
      fail();
    } catch (AssertionError error) {}
    try {
      d1015.insertPartAfter(new DiskRangeList(9, 11)); // Not a part.
      fail();
    } catch (AssertionError error) {}

    try {
      d1015.setEnd(21); // Not sequential with next.
      fail();
    } catch (AssertionError error) {}
  }

  private void fail() throws Exception {
    throw new Exception(); // Don't use Assert.fail, we are catching assertion errors.
  }
}
