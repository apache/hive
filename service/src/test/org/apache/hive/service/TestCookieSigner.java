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

package org.apache.hive.service;

import java.util.Random;



import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * CLIServiceTest.
 *
 */
public class TestCookieSigner {

  protected static CookieSigner cs;
  private static final Random RAN = new Random();

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    cs = new CookieSigner(Long.toString(RAN.nextLong()).getBytes());
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testVerifyAndExtract() throws Exception {
    String originalStr = "cu=scott";
    String signedStr = cs.signCookie(originalStr);
    assert(cs.verifyAndExtract(signedStr).equals(originalStr));
  }
}
