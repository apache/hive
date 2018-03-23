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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.security.authorization.MetaStoreAuthzAPIAuthorizerEmbedOnly;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener;
import org.junit.Test;

/**
 * Test case for {@link MetaStoreAuthzAPIAuthorizerEmbedOnly} The authorizer is
 * supposed to allow api calls for metastore in embedded mode while disallowing
 * them in remote metastore mode. Note that this is an abstract class, the
 * subclasses that set the mode and the tests here get run as part of their
 * testing.
 */
public abstract class AbstractTestAuthorizationApiAuthorizer {
  protected static boolean isRemoteMetastoreMode;
  private static HiveConf hiveConf;
  private static HiveMetaStoreClient msc;

  protected static void setup() throws Exception {
    System.err.println("Running with remoteMode = " + isRemoteMetastoreMode);
    System.setProperty("hive.metastore.pre.event.listeners",
        AuthorizationPreEventListener.class.getName());
    System.setProperty("hive.security.metastore.authorization.manager",
        MetaStoreAuthzAPIAuthorizerEmbedOnly.class.getName());

    hiveConf = new HiveConf();
    if (isRemoteMetastoreMode) {
      int port = MetaStoreTestUtils.startMetaStoreWithRetry();
      hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + port);
    }
    hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");

    msc = new HiveMetaStoreClient(hiveConf);

  }

  interface FunctionInvoker {
    public void invoke() throws Exception;
  }

  /**
   * Test the if authorization failed/passed for FunctionInvoker that invokes a metastore client
   * api call
   * @param mscFunctionInvoker
   * @throws Exception
   */
  private void testFunction(FunctionInvoker mscFunctionInvoker) throws Exception {
    boolean caughtEx = false;
    try {
      try {
        mscFunctionInvoker.invoke();
      } catch (RuntimeException e) {
        // A hack to verify that authorization check passed. Exception can be thrown be cause
        // the functions are not being called with valid params.
        // verify that exception has come from ObjectStore code, which means that the
        // authorization checks passed.
        String exStackString = ExceptionUtils.getStackTrace(e);
        assertTrue("Verifying this exception came after authorization check",
            exStackString.contains("org.apache.hadoop.hive.metastore.ObjectStore"));
        // If its not an exception caused by auth check, ignore it
      }
      assertFalse("Authz Exception should have been thrown in remote mode", isRemoteMetastoreMode);
      System.err.println("No auth exception thrown");
    } catch (MetaException e) {
      System.err.println("Caught exception");
      caughtEx = true;
      assertTrue(e.getMessage().contains(MetaStoreAuthzAPIAuthorizerEmbedOnly.errMsg));
    }
    if (!isRemoteMetastoreMode) {
      assertFalse("No exception should be thrown in embedded mode", caughtEx);
    }
  }

  @Test
  public void testGrantPriv() throws Exception {
    FunctionInvoker invoker = new FunctionInvoker() {
      @Override
      public void invoke() throws Exception {
        msc.grant_privileges(new PrivilegeBag(new ArrayList<HiveObjectPrivilege>()));
      }
    };
    testFunction(invoker);
  }

  @Test
  public void testRevokePriv() throws Exception {
    FunctionInvoker invoker = new FunctionInvoker() {
      @Override
      public void invoke() throws Exception {
        msc.revoke_privileges(new PrivilegeBag(new ArrayList<HiveObjectPrivilege>()), false);
      }
    };
    testFunction(invoker);
  }

  @Test
  public void testGrantRole() throws Exception {
    FunctionInvoker invoker = new FunctionInvoker() {
      @Override
      public void invoke() throws Exception {
        msc.grant_role(null, null, null, null, null, true);
      }
    };
    testFunction(invoker);
  }

  @Test
  public void testRevokeRole() throws Exception {
    FunctionInvoker invoker = new FunctionInvoker() {
      @Override
      public void invoke() throws Exception {
        msc.revoke_role(null, null, null, false);
      }
    };
    testFunction(invoker);
  }

  @Test
  public void testCreateRole() throws Exception {
    FunctionInvoker invoker = new FunctionInvoker() {
      @Override
      public void invoke() throws Exception {
        msc.create_role(new Role("role1", 0, "owner"));
      }
    };
    testFunction(invoker);
  }

  @Test
  public void testDropRole() throws Exception {
    FunctionInvoker invoker = new FunctionInvoker() {
      @Override
      public void invoke() throws Exception {
        msc.drop_role(null);
      }
    };
    testFunction(invoker);
  }

  @Test
  public void testListRoles() throws Exception {
    FunctionInvoker invoker = new FunctionInvoker() {
      @Override
      public void invoke() throws Exception {
        msc.list_roles(null, null);
      }
    };
    testFunction(invoker);
  }

  @Test
  public void testGetPrivSet() throws Exception {
    FunctionInvoker invoker = new FunctionInvoker() {
      @Override
      public void invoke() throws Exception {
        msc.get_privilege_set(new HiveObjectRef(), null, new ArrayList<String>());
      }
    };
    testFunction(invoker);
  }

  @Test
  public void testListPriv() throws Exception {
    FunctionInvoker invoker = new FunctionInvoker() {
      @Override
      public void invoke() throws Exception {
        msc.list_privileges(null, PrincipalType.USER, new HiveObjectRef());
      }
    };
    testFunction(invoker);
  }



}
