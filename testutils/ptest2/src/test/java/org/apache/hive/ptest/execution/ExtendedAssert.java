/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.ptest.execution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;


public final class ExtendedAssert {

  private ExtendedAssert() {
  }

  /**
   * Asserts that all the elements of the list are equivalent under equals()
   *
   * @param expected
   *          a list full of expected values
   * @param actual
   *          a list full of actual test values
   */
  public static void assertListEquals(String msg, final List<?> expected,
      final List<?> actual) {
    String cleanMsg = msg == null ? "" : msg;
    if (expected.size() != actual.size()) {
      fail(cleanMsg + ": Expected list of size " + expected.size() + "; actual size is "
          + actual.size());
    }
    for (int i = 0; i < expected.size(); i++) {
      final Object t1 = expected.get(i);
      final Object t2 = actual.get(i);

      assertEquals(cleanMsg + ": Index " + i,t1, t2);
    }
  }
}
