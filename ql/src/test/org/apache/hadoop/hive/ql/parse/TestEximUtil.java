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

package org.apache.hadoop.hive.ql.parse;


import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

/**
 * TestEximUtil.
 *
 */
public class TestEximUtil {

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testCheckCompatibility() throws SemanticException {

    // backward/forward compatible
    EximUtil.doCheckCompatibility(
        "10.3", // current code version
        "10.4", // data's version
        null // data's FC version
        ); // No exceptions expected
    EximUtil.doCheckCompatibility(
        "10.4", // current code version
        "10.4", // data's version
        null // data's FC version
        ); // No exceptions expected
    EximUtil.doCheckCompatibility(
        "10.5", // current code version
        "10.4", // data's version
        null // data's FC version
        ); // No exceptions expected

    // not backward compatible
    try {
      EximUtil.doCheckCompatibility(
          "11.0", // current code version
          "10.4", // data's version
          null // data's FC version
          ); // No exceptions expected
      fail();
    } catch (SemanticException e) {
    }

    // not forward compatible
    try {
      EximUtil.doCheckCompatibility(
          "9.9", // current code version
          "10.4", // data's version
          null // data's FC version
          ); // No exceptions expected
      fail();
    } catch (SemanticException e) {
    }

    // forward compatible
    EximUtil.doCheckCompatibility(
          "9.9", // current code version
        "10.4", // data's version
        "9.9" // data's FC version
    ); // No exceptions expected
    EximUtil.doCheckCompatibility(
        "9.9", // current code version
        "10.4", // data's version
        "9.8" // data's FC version
    ); // No exceptions expected
    EximUtil.doCheckCompatibility(
        "9.9", // current code version
        "10.4", // data's version
        "8.8" // data's FC version
    ); // No exceptions expected
    EximUtil.doCheckCompatibility(
        "10.3", // current code version
        "10.4", // data's version
        "10.3" // data's FC version
    ); // No exceptions expected

    // not forward compatible
    try {
      EximUtil.doCheckCompatibility(
          "10.2", // current code version
          "10.4", // data's version
          "10.3" // data's FC version
      ); // No exceptions expected
      fail();
    } catch (SemanticException e) {
    }

  }
}
