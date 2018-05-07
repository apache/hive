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

import org.apache.hadoop.hive.common.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Evolving;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
/**
 * Factory for getting current valid instance of IMetaStoreClient
 * Metastore client cannot be cached in authorization interface as that
 * can get invalidated between the calls with the logic in Hive class.
 * The standard way of getting metastore client object is through Hive.get().getMSC().
 * But Hive class is not a public interface, so this factory helps in hiding Hive
 * class from the authorization interface users.
 */
@LimitedPrivate(value = { "Apache Argus (incubating)" })
@Evolving
public interface HiveMetastoreClientFactory {
  IMetaStoreClient getHiveMetastoreClient() throws HiveAuthzPluginException;
}
