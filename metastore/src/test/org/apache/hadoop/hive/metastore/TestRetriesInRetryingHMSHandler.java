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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeUnit;

import javax.jdo.JDOException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class TestRetriesInRetryingHMSHandler {

  private static HiveConf hiveConf;
  private static final int RETRY_ATTEMPTS = 3;

  @BeforeClass
  public static void setup() throws IOException {
    hiveConf = new HiveConf();
    int port = MetaStoreUtils.findFreePort();
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + port);
    hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    hiveConf.setIntVar(HiveConf.ConfVars.HMSHANDLERATTEMPTS, RETRY_ATTEMPTS);
    hiveConf.setTimeVar(HiveConf.ConfVars.HMSHANDLERINTERVAL, 10, TimeUnit.MILLISECONDS);
    hiveConf.setBoolVar(HiveConf.ConfVars.HMSHANDLERFORCERELOADCONF, false);
  }

  /*
   * If the init method of HMSHandler throws exception for the first time
   * while creating RetryingHMSHandler it should be retried
   */
  @Test
  public void testRetryInit() throws MetaException {
    IHMSHandler mockBaseHandler = Mockito.mock(HiveMetaStore.HMSHandler.class);
    Mockito.when(mockBaseHandler.getConf()).thenReturn(hiveConf);
    Mockito
    .doThrow(JDOException.class)
    .doNothing()
    .when(mockBaseHandler).init();
    RetryingHMSHandler.getProxy(hiveConf, mockBaseHandler, false);
    Mockito.verify(mockBaseHandler, Mockito.times(2)).init();
  }

  /*
   * init method in HMSHandler should not be retried if there are no exceptions
   */
  @Test
  public void testNoRetryInit() throws MetaException {
    IHMSHandler mockBaseHandler = Mockito.mock(HiveMetaStore.HMSHandler.class);
    Mockito.when(mockBaseHandler.getConf()).thenReturn(hiveConf);
    Mockito.doNothing().when(mockBaseHandler).init();
    RetryingHMSHandler.getProxy(hiveConf, mockBaseHandler, false);
    Mockito.verify(mockBaseHandler, Mockito.times(1)).init();
  }

  /*
   * If the init method in HMSHandler throws exception all the times it should be retried until
   * HiveConf.ConfVars.HMSHANDLERATTEMPTS is reached before giving up
   */
  @Test(expected = MetaException.class)
  public void testRetriesLimit() throws MetaException {
    IHMSHandler mockBaseHandler = Mockito.mock(HiveMetaStore.HMSHandler.class);
    Mockito.when(mockBaseHandler.getConf()).thenReturn(hiveConf);
    Mockito.doThrow(JDOException.class).when(mockBaseHandler).init();
    RetryingHMSHandler.getProxy(hiveConf, mockBaseHandler, false);
    Mockito.verify(mockBaseHandler, Mockito.times(RETRY_ATTEMPTS)).init();
  }

  /*
   * Test retries when InvocationException wrapped in MetaException wrapped in JDOException
   * is thrown
   */
  @Test
  public void testWrappedMetaExceptionRetry() throws MetaException {
    IHMSHandler mockBaseHandler = Mockito.mock(HiveMetaStore.HMSHandler.class);
    Mockito.when(mockBaseHandler.getConf()).thenReturn(hiveConf);
    //JDOException wrapped in MetaException wrapped in InvocationException
    MetaException me = new MetaException("Dummy exception");
    me.initCause(new JDOException());
    InvocationTargetException ex = new InvocationTargetException(me);
    Mockito
    .doThrow(me)
    .doNothing()
    .when(mockBaseHandler).init();
    RetryingHMSHandler.getProxy(hiveConf, mockBaseHandler, false);
    Mockito.verify(mockBaseHandler, Mockito.times(2)).init();
  }
}
