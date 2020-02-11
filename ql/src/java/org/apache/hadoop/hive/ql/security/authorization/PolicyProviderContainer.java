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
package org.apache.hadoop.hive.ql.security.authorization;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePolicyProvider;

/**
 * Wrapper of policy provider no matter this is in authorizer v1 or v2
 */
public class PolicyProviderContainer implements Iterable<HivePolicyProvider> {
  List<HiveAuthorizer> authorizers = new ArrayList<HiveAuthorizer>();
  List<HiveMetastoreAuthorizationProvider> authorizationProviders = new ArrayList<HiveMetastoreAuthorizationProvider>();

  public void addAuthorizer(HiveAuthorizer authorizer) {
    authorizers.add(authorizer);
  }

  public void addAuthorizationProvider(HiveMetastoreAuthorizationProvider authorizationProvider) {
    authorizationProviders.add(authorizationProvider);
  }

  public int size() {
    return authorizers.size() + authorizationProviders.size();
  }

  @Override
  public Iterator<HivePolicyProvider> iterator() {
    return new PolicyIterator();
  }

  class PolicyIterator implements Iterator<HivePolicyProvider> {
    int currentAuthorizerPosition = 0;
    int authorizationProviderPosition = 0;
    @Override
    public boolean hasNext() {
      if (currentAuthorizerPosition < authorizers.size()
          || authorizationProviderPosition < authorizationProviders.size()) {
        return true;
      }
      return false;
    }

    @Override
    public HivePolicyProvider next() {
      try {
        if (currentAuthorizerPosition < authorizers.size()) {
          return authorizers.get(currentAuthorizerPosition++).getHivePolicyProvider();
        } else {
          return authorizationProviders.get(authorizationProviderPosition++).getHivePolicyProvider();
        }
      } catch (HiveAuthzPluginException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
