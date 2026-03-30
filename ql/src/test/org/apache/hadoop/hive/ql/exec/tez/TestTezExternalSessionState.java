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
package org.apache.hadoop.hive.ql.exec.tez;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicInteger;
import java.lang.reflect.Field;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.TezClient;
import org.apache.tez.client.TezClient.TezClientBuilder;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.serviceplugins.api.ServicePluginsDescriptor;
import org.junit.Test;
import org.mockito.MockedStatic;

public class TestTezExternalSessionState {

  private static SessionState createSessionState() {
    HiveConf hiveConf = new HiveConfForTest(TestTezExternalSessionState.class);
    hiveConf.set("hive.security.authorization.manager",
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdConfOnlyAuthorizerFactory");
    return SessionState.start(hiveConf);
  }

  public static class CountingExternalSessionsRegistry implements ExternalSessionsRegistry {
    private final AtomicInteger getSessionCalls = new AtomicInteger(0);
    private final AtomicInteger returnSessionCalls = new AtomicInteger(0);

    public CountingExternalSessionsRegistry(HiveConf conf) {
    }

    @Override
    public String getSession() {
      int callId = getSessionCalls.incrementAndGet();
      return "application_1_" + callId;
    }

    @Override
    public void returnSession(String appId) {
      returnSessionCalls.incrementAndGet();
    }

    @Override
    public void close() {
    }

    int getGetSessionCalls() {
      return getSessionCalls.get();
    }

    int getReturnSessionCalls() {
      return returnSessionCalls.get();
    }
  }

  @Test
  public void testConsecutiveOpenInternalCallsAreIdempotent() throws Exception {
    SessionState ss = createSessionState();
    HiveConf conf = ss.getConf();
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_REGISTRY_CLASS,
        CountingExternalSessionsRegistry.class.getName());

    TezExternalSessionState session = new TezExternalSessionState("test-session", conf);
    CountingExternalSessionsRegistry registry =
        (CountingExternalSessionsRegistry) ExternalSessionsRegistryFactory.getClient(conf);
    assertFalse("Session should start closed", session.isOpen());

    TezClientBuilder builder = mock(TezClientBuilder.class);
    TezClient tezClient = mock(TezClient.class);
    when(builder.setIsSession(anyBoolean())).thenReturn(builder);
    when(builder.setCredentials(any())).thenReturn(builder);
    when(builder.setServicePluginDescriptor(any(ServicePluginsDescriptor.class))).thenReturn(builder);
    when(builder.build()).thenReturn(tezClient);
    when(tezClient.getClientName()).thenReturn("mock-client");
    when(tezClient.getClient(any(ApplicationId.class))).thenReturn(null);

    try (MockedStatic<TezClient> tezClientMock = mockStatic(TezClient.class)) {
      tezClientMock.when(() -> TezClient.newBuilder(anyString(), any(TezConfiguration.class))).thenReturn(builder);

      session.openInternal(null, false, null, null);
      TezClient firstClient = session.getTezClient();
      assertNotNull(firstClient);
      assertTrue("Session should be open after first openInternal", session.isOpen());
      String firstAppId = getExternalAppId(session);

      session.openInternal(null, false, null, null);
      TezClient secondClient = session.getTezClient();
      assertNotNull(secondClient);
      assertTrue("Session should remain open after second openInternal", session.isOpen());
      String secondAppId = getExternalAppId(session);

      // Idempotent behavior expectation: second openInternal should be a no-op for externalAppId.
      assertSame("Consecutive openInternal calls should keep the same external app id",
          firstAppId, secondAppId);
      assertSame("Consecutive openInternal calls should keep the same Tez client instance",
          firstClient, secondClient);
    }

    assertEquals("Idempotent openInternal should only acquire one external session",
        1, registry.getGetSessionCalls());
    assertEquals("openInternal should not return sessions", 0, registry.getReturnSessionCalls());
  }

  private static String getExternalAppId(TezExternalSessionState session) throws Exception {
    Field field = TezExternalSessionState.class.getDeclaredField("externalAppId");
    field.setAccessible(true);
    return (String) field.get(session);
  }
}
