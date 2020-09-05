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

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

/**
 * Unit test class for testing Ranger Dump.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({UserGroupInformation.class})
public class TestRangerRestClient {

  @Mock
  private RangerRestClientImpl mockClient;

  @Mock
  private UserGroupInformation userGroupInformation;

  @Before
  public void setup() throws Exception {
    PowerMockito.mockStatic(UserGroupInformation.class);
    Mockito.when(UserGroupInformation.getLoginUser()).thenReturn(userGroupInformation);
    Mockito.when(userGroupInformation.doAs((PrivilegedAction<Object>) Mockito.any())).thenCallRealMethod();
    Mockito.when(mockClient.getRangerExportUrl(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
      .thenCallRealMethod();
    Mockito.when(mockClient.getRangerImportUrl(Mockito.anyString(), Mockito.anyString()))
      .thenCallRealMethod();
  }

  @Test
  public void testSuccessSimpleAuthCheckConnection() throws Exception {
    Mockito.when(UserGroupInformation.isSecurityEnabled()).thenReturn(false);
    Mockito.when(mockClient.checkConnectionPlain(Mockito.anyString())).thenReturn(true);
    Mockito.when(mockClient.checkConnection(Mockito.anyString())).thenCallRealMethod();
    mockClient.checkConnection("http://localhost:6080/ranger");
    ArgumentCaptor<String> urlCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.verify(mockClient,
      Mockito.times(1)).checkConnectionPlain(urlCaptor.capture());
    Assert.assertEquals("http://localhost:6080/ranger", urlCaptor.getValue());
    ArgumentCaptor<PrivilegedAction> privilegedActionArgumentCaptor = ArgumentCaptor.forClass(PrivilegedAction.class);
    Mockito.verify(userGroupInformation,
      Mockito.times(0)).doAs(privilegedActionArgumentCaptor.capture());
  }

  @Test
  public void testSuccessKerberosAuthCheckConnection() throws Exception {
    Mockito.when(UserGroupInformation.isSecurityEnabled()).thenReturn(true);
    Mockito.when(mockClient.checkConnectionPlain(Mockito.anyString())).thenReturn(true);
    Mockito.when(mockClient.checkConnection(Mockito.anyString())).thenCallRealMethod();
    mockClient.checkConnection("http://localhost:6080/ranger");
    ArgumentCaptor<String> urlCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.verify(mockClient,
      Mockito.times(1)).checkConnectionPlain(urlCaptor.capture());
    Assert.assertEquals("http://localhost:6080/ranger", urlCaptor.getValue());
    ArgumentCaptor<PrivilegedAction> privilegedActionArgumentCaptor = ArgumentCaptor.forClass(PrivilegedAction.class);
    Mockito.verify(userGroupInformation,
      Mockito.times(3)).doAs(privilegedActionArgumentCaptor.capture());
  }

  @Test
  public void testSuccessSimpleAuthRangerExport() throws Exception {
    Mockito.when(UserGroupInformation.isSecurityEnabled()).thenReturn(false);
    Mockito.when(mockClient.exportRangerPoliciesPlain(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
      .thenReturn(new RangerExportPolicyList());
    Mockito.when(mockClient.exportRangerPolicies(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
      .thenCallRealMethod();
    mockClient.exportRangerPolicies("http://localhost:6080/ranger", "db",
      "hive");
    ArgumentCaptor<String> urlCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> dbCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> serviceCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.verify(mockClient,
      Mockito.times(1)).exportRangerPolicies(urlCaptor.capture(), dbCaptor.capture(),
      serviceCaptor.capture());
    Assert.assertEquals("http://localhost:6080/ranger", urlCaptor.getValue());
    Assert.assertEquals("db", dbCaptor.getValue());
    Assert.assertEquals("hive", serviceCaptor.getValue());
    ArgumentCaptor<PrivilegedAction> privilegedActionArgumentCaptor = ArgumentCaptor.forClass(PrivilegedAction.class);
    Mockito.verify(userGroupInformation,
      Mockito.times(0)).doAs(privilegedActionArgumentCaptor.capture());
  }

  @Test
  public void testSuccessKerberosAuthRangerExport() throws Exception {
    Mockito.when(UserGroupInformation.isSecurityEnabled()).thenReturn(true);
    Mockito.when(mockClient.exportRangerPoliciesPlain(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
      .thenReturn(new RangerExportPolicyList());
    Mockito.when(mockClient.exportRangerPolicies(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
      .thenCallRealMethod();
    mockClient.exportRangerPolicies("http://localhost:6080/ranger", "db",
      "hive");
    ArgumentCaptor<String> urlCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> dbCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> serviceCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.verify(mockClient,
      Mockito.times(1)).exportRangerPolicies(urlCaptor.capture(), dbCaptor.capture(),
      serviceCaptor.capture());
    Assert.assertEquals("http://localhost:6080/ranger", urlCaptor.getValue());
    Assert.assertEquals("db", dbCaptor.getValue());
    Assert.assertEquals("hive", serviceCaptor.getValue());
    ArgumentCaptor<PrivilegedExceptionAction> privilegedActionArgumentCaptor = ArgumentCaptor
      .forClass(PrivilegedExceptionAction.class);
    Mockito.verify(userGroupInformation,
      Mockito.times(1)).doAs(privilegedActionArgumentCaptor.capture());
  }
}
