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

import java.util.Arrays;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.events.PreCreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateTableEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * TestMetaStorePreEventListenerWithException. Test case with exception for
 * {@link org.apache.hadoop.hive.metastore.MetaStorePreEventListener}
 */
@Category(MetastoreUnitTest.class)
public class TestMetaStorePreEventListenerWithException {
  private Configuration conf;
  private HiveMetaStoreClient msc;

  public static class PreListenerWithException extends MetaStorePreEventListener {

    public PreListenerWithException(Configuration config) {
      super(config);
    }

    @Override
    public void onEvent(PreEventContext context) throws MetaException, NoSuchObjectException,
        InvalidOperationException {
      if (context instanceof PreCreateDatabaseEvent) {
        throw new MetaException("Mock MetaException for PreCreateDatabaseEvent.");
      } else if (context instanceof PreCreateTableEvent) {
        throw new NoSuchObjectException("Mock NoSuchObjectException for PreCreateTableEvent.");
      }
    }
  }

  @Before
  public void setUp() throws MetaException {
    conf = MetastoreConf.newMetastoreConf();
    conf.set("metastore.pre.event.listeners", PreListenerWithException.class.getName());
    // Use embedded metastore to test the PreEventListener exception.
    conf.set("metastore.thrift.uris", "");
    msc = new HiveMetaStoreClient(conf);
  }

  @Test
  public void testPreListenerMetaException() {
    try {
      new DatabaseBuilder()
          .setName("test_db")
          .create(msc, conf);
      fail("should fail in PreListenerWithException.");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Mock MetaException for PreCreateDatabaseEvent."));
      Throwable rootCause = ExceptionUtils.getRootCause(e);
      assertTrue(rootCause instanceof MetaException);
      String[] rootCauseStackTrace = ExceptionUtils.getRootCauseStackTrace(e);
      assertTrue(Arrays.stream(rootCauseStackTrace)
          .anyMatch(stack -> stack.contains(PreListenerWithException.class.getName())));
    }
  }

  @Test
  public void testPreListenerNoSuchObjectException() {
    try {
      new TableBuilder()
          .setTableName("test_tbl")
          .addCol("a", "string")
          .create(msc, conf);
      fail("should fail in PreListenerWithException.");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Mock NoSuchObjectException for PreCreateTableEvent."));
      String[] rootCauseStackTrace = ExceptionUtils.getRootCauseStackTrace(e);
      Throwable rootCause = ExceptionUtils.getRootCause(e);
      assertTrue(rootCause instanceof NoSuchObjectException);
      assertTrue(Arrays.stream(rootCauseStackTrace)
          .anyMatch(stack -> stack.contains(PreListenerWithException.class.getName())));
    }
  }
}
