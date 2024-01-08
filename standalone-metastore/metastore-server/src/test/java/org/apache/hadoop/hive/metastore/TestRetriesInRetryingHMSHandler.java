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

package org.apache.hadoop.hive.metastore;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.BatchUpdateException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.concurrent.TimeUnit;

import javax.jdo.JDOException;
import javax.jdo.JDOUserException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category(MetastoreCheckinTest.class)
public class TestRetriesInRetryingHMSHandler {

  private static Configuration conf;
  private static final int RETRY_ATTEMPTS = 3;

  @BeforeClass
  public static void setup() throws IOException {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, ConfVars.HMS_HANDLER_PROXY_CLASS,
        MetastoreConf.METASTORE_RETRYING_HANDLER_CLASS);
    MetastoreConf.setLongVar(conf, ConfVars.HMS_HANDLER_ATTEMPTS, RETRY_ATTEMPTS);
    MetastoreConf.setTimeVar(conf, ConfVars.HMS_HANDLER_INTERVAL, 10, TimeUnit.MILLISECONDS);
    MetastoreConf.setBoolVar(conf, ConfVars.HMS_HANDLER_FORCE_RELOAD_CONF, false);
  }

  /*
   * If the init method of HMSHandler throws exception for the first time
   * while creating RetryingHMSHandler it should be retried
   */
  @Test
  public void testRetryInit() throws MetaException {
    IHMSHandler mockBaseHandler = Mockito.mock(IHMSHandler.class);
    Mockito.when(mockBaseHandler.getConf()).thenReturn(conf);
    Mockito
    .doThrow(JDOException.class)
    .doNothing()
    .when(mockBaseHandler).init();
    HMSHandlerProxyFactory.getProxy(conf, mockBaseHandler, false);
    Mockito.verify(mockBaseHandler, Mockito.times(2)).init();
  }

  /*
   * init method in HMSHandler should not be retried if there are no exceptions
   */
  @Test
  public void testNoRetryInit() throws MetaException {
    IHMSHandler mockBaseHandler = Mockito.mock(IHMSHandler.class);
    Mockito.when(mockBaseHandler.getConf()).thenReturn(conf);
    Mockito.doNothing().when(mockBaseHandler).init();
    HMSHandlerProxyFactory.getProxy(conf, mockBaseHandler, false);
    Mockito.verify(mockBaseHandler, Mockito.times(1)).init();
  }

  /*
   * If the init method in HMSHandler throws exception all the times it should be retried until
   * HiveConf.ConfVars.HMS_HANDLER_ATTEMPTS is reached before giving up
   */
  @Test
  public void testRetriesLimit() throws MetaException {
    IHMSHandler mockBaseHandler = Mockito.mock(IHMSHandler.class);
    Mockito.when(mockBaseHandler.getConf()).thenReturn(conf);
    Mockito.doThrow(JDOException.class).when(mockBaseHandler).init();
    try {
      HMSHandlerProxyFactory.getProxy(conf, mockBaseHandler, false);
      Assert.fail("should fail for mockBaseHandler init.");
    } catch (MetaException e) {
      // expected
    }
    Mockito.verify(mockBaseHandler, Mockito.times(RETRY_ATTEMPTS + 1)).init();
  }

  /*
   * Test retries when InvocationException wrapped in MetaException wrapped in JDOException
   * is thrown
   */
  @Test
  public void testWrappedMetaExceptionRetry() throws MetaException {
    IHMSHandler mockBaseHandler = Mockito.mock(IHMSHandler.class);
    Mockito.when(mockBaseHandler.getConf()).thenReturn(conf);
    //JDOException wrapped in MetaException wrapped in InvocationException
    MetaException me = new MetaException("Dummy exception");
    me.initCause(new JDOException());
    InvocationTargetException ex = new InvocationTargetException(me);
    Mockito
    .doThrow(me)
    .doNothing()
    .when(mockBaseHandler).init();
    HMSHandlerProxyFactory.getProxy(conf, mockBaseHandler, false);
    Mockito.verify(mockBaseHandler, Mockito.times(2)).init();
  }

  @Test
  public void testGetRootCauseInMetaException() throws MetaException {
    IHMSHandler mockBaseHandler = Mockito.mock(IHMSHandler.class);
    Mockito.when(mockBaseHandler.getConf()).thenReturn(conf);
    SQLIntegrityConstraintViolationException sqlException =
        new SQLIntegrityConstraintViolationException("Cannot delete or update a parent row");
    BatchUpdateException updateException = new BatchUpdateException(sqlException);
    NucleusDataStoreException nucleusException = new NucleusDataStoreException(
        "Clear request failed: DELETE FROM `PARTITION_PARAMS` WHERE `PART_ID`=?", updateException);
    JDOUserException jdoException = new JDOUserException(
        "One or more instances could not be deleted", nucleusException);
    // SQLIntegrityConstraintViolationException wrapped in BatchUpdateException wrapped in
    // NucleusDataStoreException wrapped in JDOUserException wrapped in MetaException wrapped in InvocationException
    MetaException me = new MetaException("Dummy exception");
    me.initCause(jdoException);
    InvocationTargetException ex = new InvocationTargetException(me);
    Mockito.doThrow(me).when(mockBaseHandler).getMS();

    IHMSHandler retryingHandler = HMSHandlerProxyFactory.getProxy(conf, mockBaseHandler, false);
    try {
      retryingHandler.getMS();
      Assert.fail("should throw the mocked MetaException");
    } catch (MetaException e) {
      Assert.assertTrue(e.getMessage().contains("java.sql.SQLIntegrityConstraintViolationException"));
    }
  }

  @Test
  public void testUnrecoverableException() throws MetaException {
    IHMSHandler mockBaseHandler = Mockito.mock(IHMSHandler.class);
    Mockito.when(mockBaseHandler.getConf()).thenReturn(conf);
    SQLIntegrityConstraintViolationException sqlException =
            new SQLIntegrityConstraintViolationException("Cannot delete or update a parent row");
    BatchUpdateException updateException = new BatchUpdateException(sqlException);
    NucleusDataStoreException nucleusException = new NucleusDataStoreException(
            "Clear request failed: DELETE FROM `PARTITION_PARAMS` WHERE `PART_ID`=?", updateException);
    JDOUserException jdoException = new JDOUserException(
            "One or more instances could not be deleted", nucleusException);
    // SQLIntegrityConstraintViolationException wrapped in BatchUpdateException wrapped in
    // NucleusDataStoreException wrapped in JDOUserException wrapped in MetaException wrapped in InvocationException
    MetaException me = new MetaException("Dummy exception");
    me.initCause(jdoException);
    InvocationTargetException ex = new InvocationTargetException(me);
    Mockito.doThrow(me).when(mockBaseHandler).getMS();

    IHMSHandler retryingHandler = HMSHandlerProxyFactory.getProxy(conf, mockBaseHandler, false);
    try {
      retryingHandler.getMS();
      Assert.fail("should throw the mocked MetaException");
    } catch (MetaException e) {
      // expected
    }
    Mockito.verify(mockBaseHandler, Mockito.times(1)).getMS();
  }

  @Test
  public void testRecoverableException() throws MetaException {
    IHMSHandler mockBaseHandler = Mockito.mock(IHMSHandler.class);
    Mockito.when(mockBaseHandler.getConf()).thenReturn(conf);
    BatchUpdateException updateException = new BatchUpdateException();
    NucleusDataStoreException nucleusException = new NucleusDataStoreException(
            "Clear request failed: DELETE FROM `PARTITION_PARAMS` WHERE `PART_ID`=?", updateException);
    JDOUserException jdoException = new JDOUserException(
            "One or more instances could not be deleted", nucleusException);
    // BatchUpdateException wrapped in NucleusDataStoreException wrapped in
    // JDOUserException wrapped in MetaException wrapped in InvocationException
    MetaException me = new MetaException("Dummy exception");
    me.initCause(jdoException);
    InvocationTargetException ex = new InvocationTargetException(me);
    Mockito.doThrow(me).when(mockBaseHandler).getMS();

    IHMSHandler retryingHandler = HMSHandlerProxyFactory.getProxy(conf, mockBaseHandler, false);
    try {
      retryingHandler.getMS();
      Assert.fail("should throw the mocked MetaException");
    } catch (MetaException e) {
      // expected
    }
    Mockito.verify(mockBaseHandler, Mockito.times(RETRY_ATTEMPTS + 1)).getMS();
  }
}
