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
import org.junit.Test;

import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.thrift.TException;

import static org.apache.hadoop.hive.metastore.ExceptionHandler.handleException;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestExceptionHandler {

  @Test
  public void testThrowIfInstance() {

    MetaException me = new MetaException("MetaException test");
    try {
      handleException(me).throwIfInstance(RuntimeException.class);
    } catch (Exception e) {
      fail("Exception should not happen:" + e.getMessage());
    }

    try {
      handleException(me).throwIfInstance(MetaException.class);
      fail("Should throw a exception here");
    } catch (Exception e) {
      assertTrue(e == me);
    }

    InvalidOperationException ioe = new InvalidOperationException("InvalidOperationException test");
    try {
      handleException(ioe).throwIfInstance(MetaException.class, InvalidOperationException.class);
      fail("Should throw a exception here");
    } catch (Exception e) {
      assertTrue(e == ioe);
    }

    TException te = new TException("TException");
    try {
      handleException(te).throwIfInstance(MetaException.class, InvalidOperationException.class)
          .throwIfInstance(TException.class).defaultMetaException();
    } catch (Exception e) {
      assertTrue(e == te);
    }

    RuntimeException re = new RuntimeException("RuntimeException test");
    try {
      Exception e = handleException(re).throwIfInstance(MetaException.class, InvalidOperationException.class)
          .throwIfInstance(TException.class).defaultRuntimeException();
      assertTrue(e == re);
    } catch (Exception e) {
      fail("Exception should not happen:" + e.getMessage());
    }

    NullPointerException npe = new NullPointerException();
    try {
      Exception e = handleException(npe).throwIfInstance(MetaException.class, InvalidOperationException.class)
          .throwIfInstance(TException.class).defaultMetaException();
      assertTrue(e instanceof MetaException);
      assertTrue(e.getMessage().equals(npe.toString()));
    } catch (Exception e) {
      fail("Exception should not happen:" + e.getMessage());
    }

    try {
      handleException(me).throwIfInstance(MetaException.class, InvalidOperationException.class)
          .throwIfInstance(TException.class).defaultMetaException();
      fail("Should throw a exception here");
    } catch (Exception e) {
      assertTrue(e == me);
    }
  }

  @Test
  public void testConvertIfInstance() {
    IOException ix = new IOException("IOException test");
    try {
      handleException(ix).convertIfInstance(NoSuchObjectException.class, MetaException.class);
    } catch (Exception e) {
      fail("Exception should not happen:" + e.getMessage());
    }

    try {
      handleException(ix).convertIfInstance(IOException.class, MetaException.class);
      fail("Should throw a exception here");
    } catch (Exception e) {
      assertTrue(e instanceof MetaException);
      assertTrue(e.getMessage().equals(ix.getMessage()));
    }

    try {
      handleException(ix).convertIfInstance(NoSuchObjectException.class, MetaException.class)
          .convertIfInstance(IOException.class, MetaException.class);
      fail("Should throw a exception here");
    } catch (Exception e) {
      assertTrue(e instanceof MetaException);
      assertTrue(e.getMessage().equals(ix.getMessage()));
    }
  }
}
