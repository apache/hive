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

import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MetastoreCheckinTest.class)
public class TestRawStoreProxy {

  static class TestStore extends ObjectStore {
    @Override
    public void setConf(Configuration conf) {
      // noop
    }

    public void noopMethod() throws MetaException {
      Deadline.checkTimeout();
    }

    public void exceptions() throws IllegalStateException, MetaException {
      Deadline.checkTimeout();
      throw new IllegalStateException("throwing an exception");
    }
  }

  @Test
  public void testExceptionDispatch() throws Throwable {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setTimeVar(conf, MetastoreConf.ConfVars.CLIENT_SOCKET_TIMEOUT, 10,
        TimeUnit.MILLISECONDS);
    RawStoreProxy rsp = new RawStoreProxy(conf, conf, TestStore.class, 1);
    try {
      rsp.invoke(null, TestStore.class.getMethod("exceptions"), new Object[] {});
      fail("an exception is expected");
    } catch (IllegalStateException ise) {
      // expected
    }
    Thread.sleep(20);
    // this shouldn't throw an exception
    rsp.invoke(null, TestStore.class.getMethod("noopMethod"), new Object[] {});
  }
}
