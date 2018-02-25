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
package org.apache.hadoop.hive.ql.security.authorization.plugin;


import org.apache.hadoop.hive.common.classification.InterfaceAudience.Private;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
/**
 * Private implementaiton that returns instance of IMetaStoreClient
 */
@Private
public class HiveMetastoreClientFactoryImpl implements HiveMetastoreClientFactory{

  @Override
  public IMetaStoreClient getHiveMetastoreClient() throws HiveAuthzPluginException {
    String errMsg = "Error getting metastore client";
    try {
      return Hive.get().getMSC();
    } catch (MetaException e) {
      throw new HiveAuthzPluginException(errMsg, e);
    } catch (HiveException e) {
      throw new HiveAuthzPluginException(errMsg, e);
    }
  }

}
