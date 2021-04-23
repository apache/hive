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

import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.thrift.TException;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.hive.metastore.ExceptionHandler.convertIfInstance;
import static org.apache.hadoop.hive.metastore.ExceptionHandler.throwIfInstance;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestExceptionHandler {

  @Test
  public void testThrowIfInstance() {
    MetaException me = new MetaException("MetaException test");
    InvalidOperationException ioe = new InvalidOperationException("InvalidOperationException test");
    TException te = new TException("TException");
    NullPointerException npe = new NullPointerException();
    RuntimeException re = new RuntimeException("RuntimeException test");

    try {
      throwIfInstance(me, RuntimeException.class);
    } catch (Exception e) {
      fail("Exception should not happen:" + e.getMessage());
    }

    try {
      throwIfInstance(me, MetaException.class);
      fail("Should throw a exception here");
    } catch (Exception e) {
      assertTrue(e == me);
    }

    try {
      throwIfInstance(ioe, MetaException.class, InvalidOperationException.class);
      fail("Should throw a exception here");
    } catch (Exception e) {
      assertTrue(e == ioe);
    }

    try {
      throwIfInstance(te, MetaException.class, InvalidOperationException.class)
          .throwIfInstance(te, TException.class).defaultMetaException();
    } catch (Exception e) {
      assertTrue(e == te);
    }

    try {
      Exception e = throwIfInstance(re, MetaException.class, InvalidOperationException.class)
          .throwIfInstance(re, TException.class).defaultRuntimeException();
      assertTrue(e == re);
    } catch (Exception e) {
      fail("Exception should not happen:" + e.getMessage());
    }

    try {
      Exception e = throwIfInstance(npe, MetaException.class, InvalidOperationException.class)
          .throwIfInstance(npe, TException.class).defaultMetaException();
      assertTrue(e instanceof MetaException);
      assertTrue(e.getMessage().equals(npe.toString()));
    } catch (Exception e) {
      fail("Exception should not happen:" + e.getMessage());
    }

    try {
      throwIfInstance(me, MetaException.class, InvalidOperationException.class)
          .throwIfInstance(me, TException.class).defaultMetaException();
      fail("Should throw a exception here");
    } catch (Exception e) {
      assertTrue(e == me);
    }
  }

  @Test
  public void testConvertIfInstance() {
    IOException ix = new IOException("IOException test");
    try {
      convertIfInstance(ix, NoSuchObjectException.class, MetaException.class);
    } catch (Exception e) {
      fail("Exception should not happen:" + e.getMessage());
    }

    try {
      convertIfInstance(ix, IOException.class, MetaException.class);
      fail("Should throw a exception here");
    } catch (Exception e) {
      assertTrue(e instanceof MetaException);
      assertTrue(e.getMessage().equals(ix.getMessage()));
    }

    try {
      convertIfInstance(ix, NoSuchObjectException.class, MetaException.class)
          .convertIfInstance(ix, IOException.class, MetaException.class);
      fail("Should throw a exception here");
    } catch (Exception e) {
      assertTrue(e instanceof MetaException);
      assertTrue(e.getMessage().equals(ix.getMessage()));
    }
  }

}
