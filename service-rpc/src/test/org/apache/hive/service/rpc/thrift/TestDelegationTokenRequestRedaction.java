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
package org.apache.hive.service.rpc.thrift;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestDelegationTokenRequestRedaction {
  private static final String LIVE_TOKEN = "LIVE_DELEGATION_TOKEN_ABC123";

  @Test
  public void testCancelReqToStringRedactsDelegationToken() {
    TCancelDelegationTokenReq req = new TCancelDelegationTokenReq();
    req.setDelegationToken(LIVE_TOKEN);

    String reqString = req.toString();
    assertTrue(reqString.contains("delegationToken:"));
    assertTrue(reqString.toLowerCase().contains("redacted"));
    assertFalse(reqString.contains(LIVE_TOKEN));
  }

  @Test
  public void testRenewReqToStringRedactsDelegationToken() {
    TRenewDelegationTokenReq req = new TRenewDelegationTokenReq();
    req.setDelegationToken(LIVE_TOKEN);

    String reqString = req.toString();
    assertTrue(reqString.contains("delegationToken:"));
    assertTrue(reqString.toLowerCase().contains("redacted"));
    assertFalse(reqString.contains(LIVE_TOKEN));
  }
}
