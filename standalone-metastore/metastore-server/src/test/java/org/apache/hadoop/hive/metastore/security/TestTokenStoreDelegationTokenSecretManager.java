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
package org.apache.hadoop.hive.metastore.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Test the renewal of Delegation tokens obtained from the Metastore.
 */
@Category(MetastoreUnitTest.class) public class TestTokenStoreDelegationTokenSecretManager {
  private final Configuration conf = MetastoreConf.newMetastoreConf();

  private TokenStoreDelegationTokenSecretManager createTokenMgr(DelegationTokenStore tokenStore,
      long renewSecs) {
    MetastoreConf.setTimeVar(conf, MetastoreConf.ConfVars.DELEGATION_TOKEN_RENEW_INTERVAL,
        renewSecs, TimeUnit.SECONDS);
    long secretKeyInterval =
        MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.DELEGATION_KEY_UPDATE_INTERVAL,
            TimeUnit.MILLISECONDS);
    long tokenMaxLifetime =
        MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.DELEGATION_TOKEN_MAX_LIFETIME,
            TimeUnit.MILLISECONDS);
    long tokenRenewInterval =
        MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.DELEGATION_TOKEN_RENEW_INTERVAL,
            TimeUnit.MILLISECONDS);
    long tokenGcInterval =
        MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.DELEGATION_TOKEN_GC_INTERVAL,
            TimeUnit.MILLISECONDS);
    return new TokenStoreDelegationTokenSecretManager(secretKeyInterval, tokenMaxLifetime,
        tokenRenewInterval, tokenGcInterval, tokenStore);
  }

  private DelegationTokenIdentifier getID(String tokenStr) throws IOException {
    DelegationTokenIdentifier id = new DelegationTokenIdentifier();
    Token<DelegationTokenIdentifier> token = new Token<>();
    token.decodeFromUrlString(tokenStr);
    try (DataInputStream in = new DataInputStream(
        new ByteArrayInputStream(token.getIdentifier()))) {
      id.readFields(in);
    }
    return id;
  }

  @Test public void testRenewal() throws IOException, InterruptedException {
    DelegationTokenStore tokenStore = new MemoryTokenStore();
    // Have a long renewal to ensure that Thread.sleep does not overshoot the initial validity
    TokenStoreDelegationTokenSecretManager mgr = createTokenMgr(tokenStore, 3600);
    try {
      mgr.startThreads();
      String tokenStr =
          mgr.getDelegationToken(UserGroupInformation.getCurrentUser().getShortUserName(),
              UserGroupInformation.getCurrentUser().getShortUserName());
      Assert.assertNotNull(mgr.verifyDelegationToken(tokenStr));
      DelegationTokenIdentifier id = getID(tokenStr);
      long initialExpiry = tokenStore.getToken(id).getRenewDate();
      Thread.sleep(100);
      Assert.assertTrue(System.currentTimeMillis() > id.getIssueDate());
      // No change in renewal date without renewal
      Assert.assertEquals(tokenStore.getToken(id).getRenewDate(), initialExpiry);
      mgr.renewDelegationToken(tokenStr);
      // Verify the token is valid
      Assert.assertNotNull(mgr.verifyDelegationToken(tokenStr));
      // Renewal date has increased after renewal
      Assert.assertTrue(tokenStore.getToken(id).getRenewDate() > initialExpiry);
    } finally {
      mgr.stopThreads();
    }
  }

  @Test public void testExpiry() throws IOException, InterruptedException {
    DelegationTokenStore tokenStore = new MemoryTokenStore();
    TokenStoreDelegationTokenSecretManager mgr = createTokenMgr(tokenStore, 1);
    try {
      mgr.startThreads();
      String tokenStr =
          mgr.getDelegationToken(UserGroupInformation.getCurrentUser().getShortUserName(),
              UserGroupInformation.getCurrentUser().getShortUserName());
      DelegationTokenIdentifier id = getID(tokenStr);
      Assert.assertNotNull(mgr.verifyDelegationToken(tokenStr));
      // Sleep for the renewal duration
      Thread.sleep(1000);
      SecretManager.InvalidToken ex = Assert.assertThrows(SecretManager.InvalidToken.class,
          () -> mgr.verifyDelegationToken(tokenStr));
      Assert.assertTrue(ex.getMessage().contains("is expired"));
    } finally {
      mgr.stopThreads();
    }
  }
}
