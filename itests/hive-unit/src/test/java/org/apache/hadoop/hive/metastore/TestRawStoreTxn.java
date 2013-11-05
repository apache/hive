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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import javax.jdo.JDOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

public class TestRawStoreTxn extends TestCase {

  public static class DummyRawStoreWithCommitError extends DummyRawStoreForJdoConnection {
    private static int callCount = 0;

    @Override
    /***
     * Throw exception on first try
     */
    public boolean commitTransaction() {
      callCount++;
      if (callCount == 1 ) {
        throw new JDOException ("Failed for call count " + callCount);
      } else {
        return true;
      }
    }
  }

  private ObjectStore objStore;
  private HiveConf hiveConf;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    hiveConf = new HiveConf();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  /***
   * Check annotations of the restricted methods
   * @throws Exception
   */
  public void testCheckNoRetryMethods() throws Exception {
    List<String> nonExecMethods =
      Arrays.asList("commitTransaction", "commitTransaction");

    RawStore rawStore = RetryingRawStore.getProxy(hiveConf, new Configuration(hiveConf),
          hiveConf.getVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL), 1);
    for (Method rawStoreMethod : RawStore.class.getMethods()) {
      if (nonExecMethods.contains(rawStoreMethod.getName())) {
        assertNotNull(rawStoreMethod.getAnnotation(RawStore.CanNotRetry.class));
      }
    }
  }

  /***
   * Invoke commit and verify it doesn't get retried
   * @throws Exception
   */
  public void testVerifyNoRetryMethods() throws Exception {
    hiveConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY,
        DummyJdoConnectionUrlHook.newUrl);;
    hiveConf.setVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL,
        DummyRawStoreWithCommitError.class.getName());
    RawStore rawStore = RetryingRawStore.getProxy(hiveConf, new Configuration(hiveConf),
        DummyRawStoreWithCommitError.class.getName(), 1);
    try {
      rawStore.commitTransaction();
      fail("Commit should fail due to no retry");
    } catch (JDOException e) {
      // Excepted JDOException
    }
  }

}
