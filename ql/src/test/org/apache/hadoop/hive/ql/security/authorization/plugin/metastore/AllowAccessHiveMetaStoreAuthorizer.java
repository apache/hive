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

package org.apache.hadoop.hive.ql.security.authorization.plugin.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.security.HiveMetastoreAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.security.UserGroupInformation;
import org.mockito.Mockito;

import java.util.List;

/*
Dummy HiveMetaStoreAuthorizer to check whether HiveMetaStoreAuthzInfo is getting created by HiveMetaStoreAuthorizer
 */



public class AllowAccessHiveMetaStoreAuthorizer extends HiveMetaStoreAuthorizer {
  public AllowAccessHiveMetaStoreAuthorizer(Configuration config) {
    super(config);
  }

  private String user = null;

  private static final ThreadLocal<Configuration> tConfig = new ThreadLocal<Configuration>() {
    @Override
    protected Configuration initialValue() {
      return new HiveConf(HiveMetaStoreAuthorizer.class);
    }
  };

  private static final ThreadLocal<HiveMetastoreAuthenticationProvider> tAuthenticator = new ThreadLocal<HiveMetastoreAuthenticationProvider>() {
    @Override
    protected HiveMetastoreAuthenticationProvider initialValue() {
      try {
        return (HiveMetastoreAuthenticationProvider) HiveUtils.getAuthenticator(tConfig.get(), HiveConf.ConfVars.HIVE_METASTORE_AUTHENTICATOR_MANAGER);
      } catch (HiveException excp) {
        throw new IllegalStateException("Authentication provider instantiation failure", excp);
      }
    }
  };

  @Override
  HiveMetaStoreAuthzInfo buildAuthzContext(PreEventContext preEventContext) throws MetaException {
    HiveMetaStoreAuthzInfo hiveMetaStoreAuthzInfo = null;
    try {
      user = UserGroupInformation.getLoginUser().getShortUserName();
    } catch (Exception e) {
        throw new MetaException("User not found: " + user);
    }

    try {
      if (!isSuperUser(user)) {
        hiveMetaStoreAuthzInfo = super.buildAuthzContext(preEventContext);
        HiveConf hiveConf = new HiveConf(super.getConf(), HiveConf.class);
        HiveAuthorizerFactory authorizerFactory = HiveUtils.getAuthorizerFactory(hiveConf, HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER);

        if (authorizerFactory != null) {
          HiveMetastoreAuthenticationProvider authenticator = tAuthenticator.get();

          authenticator.setConf(hiveConf);

          HiveAuthzSessionContext.Builder authzContextBuilder = new HiveAuthzSessionContext.Builder();

          authzContextBuilder.setClientType(HiveAuthzSessionContext.CLIENT_TYPE.HIVEMETASTORE);
          authzContextBuilder.setSessionString("HiveMetaStore");

          HiveAuthorizer hiveAuthorizer = Mockito.mock(HiveAuthorizer.class);

          HiveOperationType hiveOpType = hiveMetaStoreAuthzInfo.getOperationType();
          List<HivePrivilegeObject> inputHObjs = hiveMetaStoreAuthzInfo.getInputHObjs();
          List<HivePrivilegeObject> outputHObjs = hiveMetaStoreAuthzInfo.getOutputHObjs();
          HiveAuthzContext hiveAuthzContext = hiveMetaStoreAuthzInfo.getHiveAuthzContext();

          Mockito.doThrow(new Exception())
                  .when(hiveAuthorizer)
                  .checkPrivileges(hiveOpType, inputHObjs, outputHObjs, hiveAuthzContext);

        }
      }
    } catch (Exception e) {
      // Exception not throw to simulate Allow Access.
    }
    return hiveMetaStoreAuthzInfo;
  }
}


