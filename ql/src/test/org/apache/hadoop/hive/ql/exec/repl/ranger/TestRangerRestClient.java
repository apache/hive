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

package org.apache.hadoop.hive.ql.exec.repl.ranger;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.config.ClientConfig;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mockStatic;


/**
 * Unit test class for testing Ranger Dump.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestRangerRestClient {

  @Mock
  private RangerRestClientImpl mockClient;

  @Mock
  private UserGroupInformation userGroupInformation;

  @Mock
  private HiveConf conf;

  MockedStatic<UserGroupInformation> userGroupInformationMockedStatic;

  @Before
  public void setup() throws Exception {
    userGroupInformationMockedStatic = mockStatic(UserGroupInformation.class);

    userGroupInformationMockedStatic.when(UserGroupInformation::getLoginUser).thenReturn(userGroupInformation);

    Mockito.when(mockClient.getRangerExportUrl(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
            .thenCallRealMethod();
    Mockito.when(conf.getTimeVar(HiveConf.ConfVars.REPL_RETRY_INTIAL_DELAY, TimeUnit.SECONDS)).thenReturn(1L);
    Mockito.when(conf.getTimeVar(HiveConf.ConfVars.REPL_RETRY_TOTAL_DURATION, TimeUnit.SECONDS)).thenReturn(20L);
    Mockito.when(conf.getTimeVar(HiveConf.ConfVars.REPL_RETRY_JITTER, TimeUnit.SECONDS)).thenReturn(1L);
    Mockito.when(conf.getTimeVar(HiveConf.ConfVars.REPL_RETRY_MAX_DELAY_BETWEEN_RETRIES, TimeUnit.SECONDS))
            .thenReturn(10L);
  }

  @After
  public void tearDown() {
    userGroupInformationMockedStatic.close();
  }

  @Test
  public void testSuccessSimpleAuthCheckConnection() throws Exception {
    Mockito.when(UserGroupInformation.isSecurityEnabled()).thenReturn(false);
    Mockito.when(mockClient.checkConnectionPlain(Mockito.anyString(), Mockito.any(HiveConf.class))).thenReturn(true);
    Mockito.when(mockClient.checkConnection(Mockito.anyString(), Mockito.any())).thenCallRealMethod();
    mockClient.checkConnection("http://localhost:6080/ranger", conf);
    ArgumentCaptor<String> urlCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.verify(mockClient,
      Mockito.times(1)).checkConnectionPlain(urlCaptor.capture(), Mockito.any(HiveConf.class));
    Assert.assertEquals("http://localhost:6080/ranger", urlCaptor.getValue());
    ArgumentCaptor<PrivilegedAction> privilegedActionArgumentCaptor = ArgumentCaptor.forClass(PrivilegedAction.class);
    Mockito.verify(userGroupInformation,
      Mockito.times(0)).doAs(privilegedActionArgumentCaptor.capture());
  }

  @Test
  public void testSuccessSimpleAuthRangerExport() throws Exception {
    Mockito.when(UserGroupInformation.isSecurityEnabled()).thenReturn(false);
    Mockito.when(mockClient.exportRangerPoliciesPlain(Mockito.anyString(),
            Mockito.any(HiveConf.class))).thenReturn(new RangerExportPolicyList());
    Mockito.when(mockClient.exportRangerPolicies(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
      Mockito.any()))
      .thenCallRealMethod();
    mockClient.exportRangerPolicies("http://localhost:6080/ranger", "db",
      "hive", conf);
    ArgumentCaptor<String> urlCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> dbCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> serviceCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<HiveConf> confCaptor = ArgumentCaptor.forClass(HiveConf.class);
    Mockito.verify(mockClient,
      Mockito.times(1)).exportRangerPolicies(urlCaptor.capture(), dbCaptor.capture(),
      serviceCaptor.capture(), confCaptor.capture());
    Assert.assertEquals("http://localhost:6080/ranger", urlCaptor.getValue());
    Assert.assertEquals("db", dbCaptor.getValue());
    Assert.assertEquals("hive", serviceCaptor.getValue());
    ArgumentCaptor<PrivilegedAction> privilegedActionArgumentCaptor = ArgumentCaptor.forClass(PrivilegedAction.class);
    Mockito.verify(userGroupInformation,
      Mockito.times(0)).doAs(privilegedActionArgumentCaptor.capture());
  }

  @Test
  public void testRangerClientTimeouts() {
    Mockito.when(conf.getTimeVar(HiveConf.ConfVars.REPL_EXTERNAL_CLIENT_CONNECT_TIMEOUT,
            TimeUnit.MILLISECONDS)).thenReturn(20L);
    Mockito.when(conf.getTimeVar(HiveConf.ConfVars.REPL_RANGER_CLIENT_READ_TIMEOUT,
            TimeUnit.MILLISECONDS)).thenReturn(500L);
    Mockito.when(mockClient.getRangerClient(Mockito.any(HiveConf.class))).thenCallRealMethod();
    Client client =mockClient.getRangerClient(conf);
    Assert.assertEquals(20, client.getProperties().get(ClientConfig.PROPERTY_CONNECT_TIMEOUT));
    Assert.assertEquals(500, client.getProperties().get(ClientConfig.PROPERTY_READ_TIMEOUT));
  }
}
