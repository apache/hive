/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.hcatalog.streaming.mutate.client;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hive.hcatalog.streaming.TransactionBatch;
import org.apache.hive.hcatalog.streaming.mutate.client.lock.Lock;
import org.apache.hive.hcatalog.streaming.mutate.client.lock.LockException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestTransaction {

  private static final String USER = "user";
  private static final long TRANSACTION_ID = 10L;

  @Mock
  private Lock mockLock;
  @Mock
  private IMetaStoreClient mockMetaStoreClient;

  private Transaction transaction;

  @Before
  public void createTransaction() throws Exception {
    when(mockLock.getUser()).thenReturn(USER);
    when(mockMetaStoreClient.openTxn(USER)).thenReturn(TRANSACTION_ID);
    transaction = new Transaction(mockMetaStoreClient, mockLock);
  }

  @Test
  public void testInitialState() {
    assertThat(transaction.getState(), is(TransactionBatch.TxnState.INACTIVE));
    assertThat(transaction.getTransactionId(), is(TRANSACTION_ID));
  }

  @Test
  public void testBegin() throws Exception {
    transaction.begin();

    verify(mockLock).acquire(TRANSACTION_ID);
    assertThat(transaction.getState(), is(TransactionBatch.TxnState.OPEN));
  }

  @Test
  public void testBeginLockFails() throws Exception {
    doThrow(new LockException("")).when(mockLock).acquire(TRANSACTION_ID);

    try {
      transaction.begin();
    } catch (TransactionException ignore) {
    }

    assertThat(transaction.getState(), is(TransactionBatch.TxnState.INACTIVE));
  }

  @Test
  public void testCommit() throws Exception {
    transaction.commit();

    verify(mockLock).release();
    verify(mockMetaStoreClient).commitTxn(TRANSACTION_ID);
    assertThat(transaction.getState(), is(TransactionBatch.TxnState.COMMITTED));
  }

  @Test(expected = TransactionException.class)
  public void testCommitLockFails() throws Exception {
    doThrow(new LockException("")).when(mockLock).release();
    transaction.commit();
  }

  @Test
  public void testAbort() throws Exception {
    transaction.abort();

    verify(mockLock).release();
    verify(mockMetaStoreClient).rollbackTxn(TRANSACTION_ID);
    assertThat(transaction.getState(), is(TransactionBatch.TxnState.ABORTED));
  }

  @Test(expected = TransactionException.class)
  public void testAbortLockFails() throws Exception {
    doThrow(new LockException("")).when(mockLock).release();
    transaction.abort();
  }

}
