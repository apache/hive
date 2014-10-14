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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.pig;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * Test utilities for selectively disabling specific test methods for given storage formats.
 */
public class TestUtil {
  private static final Map<String, Set<String>> SAMPLE_DISABLED_TESTS_MAP =
      new HashMap<String, Set<String>>() {{
        put("test", new HashSet<String>() {{
          add("testShouldSkip");
        }});
      }};

  /**
   * Determine whether the caller test method is in a set of disabled test methods for a given
   * storage format.
   *
   * @param storageFormat The name of the storage format used in a STORED AS clause.
   * @param disabledTestsMap Map of storage format name to set of test method names that indicate
   *        which test methods should not run against the given storage format.
   * @return True if the caller test method should be skipped for the given storage format.
   */
  public static boolean shouldSkip(String storageFormat, Map<String, Set<String>> disabledTestsMap) {
    final StackTraceElement[] elements = Thread.currentThread().getStackTrace();
    // The "bottom" of the call stack is at the front of the array. The elements are as follows:
    //   [0] getStackTrace()
    //   [1] shouldSkip()
    //   [2] caller test method
    String methodName = elements[2].getMethodName();
    if (!disabledTestsMap.containsKey(storageFormat)) {
      return false;
    }

    Set<String> disabledMethods = disabledTestsMap.get(storageFormat);
    return disabledMethods.contains(methodName);
  }

  @Test
  public void testShouldSkip() {
    assertTrue(TestUtil.shouldSkip("test", SAMPLE_DISABLED_TESTS_MAP));
  }

  @Test
  public void testShouldNotSkip() {
    assertFalse(TestUtil.shouldSkip("test", SAMPLE_DISABLED_TESTS_MAP));
    assertFalse(TestUtil.shouldSkip("foo", SAMPLE_DISABLED_TESTS_MAP));
  }
}
