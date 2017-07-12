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
package org.apache.hive.hcatalog.streaming.mutate.worker;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat.Options;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestMutatorImpl {

  private static final Object RECORD = new Object();
  private static final int RECORD_ID_COLUMN = 2;
  private static final int BUCKET_ID = 0;
  private static final Path PATH = new Path("X");
  private static final long TRANSACTION_ID = 1L;

  @Mock
  private AcidOutputFormat<?, ?> mockOutputFormat;
  @Mock
  private ObjectInspector mockObjectInspector;
  @Mock
  private RecordUpdater mockRecordUpdater;
  @Captor
  private ArgumentCaptor<AcidOutputFormat.Options> captureOptions;

  private final HiveConf configuration = new HiveConf();

  private Mutator mutator;

  @Before
  public void injectMocks() throws IOException {
    when(mockOutputFormat.getRecordUpdater(eq(PATH), any(Options.class))).thenReturn(mockRecordUpdater);
    mutator = new MutatorImpl(configuration, RECORD_ID_COLUMN, mockObjectInspector, mockOutputFormat, TRANSACTION_ID,
        PATH, BUCKET_ID);
  }

  @Test
  public void testCreatesRecordReader() throws IOException {
    verify(mockOutputFormat).getRecordUpdater(eq(PATH), captureOptions.capture());
    Options options = captureOptions.getValue();
    assertThat(options.getBucketId(), is(BUCKET_ID));
    assertThat(options.getConfiguration(), is((Configuration) configuration));
    assertThat(options.getInspector(), is(mockObjectInspector));
    assertThat(options.getRecordIdColumn(), is(RECORD_ID_COLUMN));
    assertThat(options.getMinimumTransactionId(), is(TRANSACTION_ID));
    assertThat(options.getMaximumTransactionId(), is(TRANSACTION_ID));
  }

  @Test
  public void testInsertDelegates() throws IOException {
    mutator.insert(RECORD);
    verify(mockRecordUpdater).insert(TRANSACTION_ID, RECORD);
  }

  @Test
  public void testUpdateDelegates() throws IOException {
    mutator.update(RECORD);
    verify(mockRecordUpdater).update(TRANSACTION_ID, RECORD);
  }

  @Test
  public void testDeleteDelegates() throws IOException {
    mutator.delete(RECORD);
    verify(mockRecordUpdater).delete(TRANSACTION_ID, RECORD);
  }

  @Test
  public void testCloseDelegates() throws IOException {
    mutator.close();
    verify(mockRecordUpdater).close(false);
  }

  @Test
  public void testFlushDoesNothing() throws IOException {
    mutator.flush();
    verify(mockRecordUpdater, never()).flush();
  }

}
