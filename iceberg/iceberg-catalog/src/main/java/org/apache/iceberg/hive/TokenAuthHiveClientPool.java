/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.hive;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * HiveClientPool that authenticates to a remote secure HMS as the current proxy user using
 * a metastore delegation token obtained by the catalog service principal.
 */
class TokenAuthHiveClientPool extends HiveClientPool {

  static final String DELEGATION_TOKEN_SERVICE = "DelegationTokenForHiveMetaStoreServer";

  private final HiveConf hiveConf;

  TokenAuthHiveClientPool(int poolSize, Configuration conf) {
    super(poolSize, conf);
    this.hiveConf = super.hiveConf();
  }

  private void setDelegationToken() {
    IMetaStoreClient tokenClient = null;
    try {
      UserGroupInformation currentUgi = SecurityUtils.getUGI();
      if (currentUgi.getAuthenticationMethod() != UserGroupInformation.AuthenticationMethod.PROXY) {
        return;
      }
      hiveConf.unset(MetastoreConf.ConfVars.TOKEN_SIGNATURE.getVarname());
      UserGroupInformation realUser = currentUgi.getRealUser();
      if (realUser == null) {
        realUser = UserGroupInformation.getLoginUser();
      }
      final UserGroupInformation tokenFetcher = realUser;
      tokenClient =
          tokenFetcher.doAs(
              (PrivilegedExceptionAction<IMetaStoreClient>)
                  () -> new HiveMetaStoreClient(hiveConf));
      String proxyUser = currentUgi.getUserName();
      String delegationTokenStr = tokenClient.getDelegationToken(proxyUser, proxyUser);
      SecurityUtils.setTokenStr(currentUgi, delegationTokenStr, DELEGATION_TOKEN_SERVICE);
      MetastoreConf.setVar(hiveConf, MetastoreConf.ConfVars.TOKEN_SIGNATURE, DELEGATION_TOKEN_SERVICE);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeMetaException(e, "Failed to get metastore delegation token");
    } catch (IOException e) {
      throw new RuntimeMetaException(e, "Failed to get metastore delegation token");
    } catch (Exception e) {
      throw new RuntimeMetaException(e, "Failed to get metastore delegation token");
    } finally {
      if (tokenClient != null) {
        tokenClient.close();
      }
    }
  }

  @Override
  protected IMetaStoreClient newClient() {
    setDelegationToken();
    return super.newClient();
  }

  @Override
  protected IMetaStoreClient reconnect(IMetaStoreClient client) {
    setDelegationToken();
    return super.reconnect(client);
  }
}
