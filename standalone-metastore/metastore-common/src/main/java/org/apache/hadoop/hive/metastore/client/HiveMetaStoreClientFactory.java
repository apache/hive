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

package org.apache.hadoop.hive.metastore.client;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory class creating a MetaStoreClient specified in a given configuration.
 */
public class HiveMetaStoreClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HiveMetaStoreClientFactory.class);

  public static IMetaStoreClient newClient(Configuration conf, boolean allowEmbedded) throws MetaException {
    String mscClassName = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.METASTORE_CLIENT_CLASS);
    LOG.info("Using {} as a base MetaStoreClient", mscClassName);
    Class<? extends IMetaStoreClient> mscClass = JavaUtils.getClass(mscClassName, IMetaStoreClient.class);

    IMetaStoreClient baseMetaStoreClient = null;
    try {
      baseMetaStoreClient = JavaUtils.newInstance(mscClass,
          new Class[]{Configuration.class, boolean.class},
          new Object[]{conf, allowEmbedded});
    } catch (Throwable t) {
      // Reflection by JavaUtils will throw RuntimeException, try to get real MetaException here.
      Throwable rootCause = ExceptionUtils.getRootCause(t);
      if (rootCause instanceof MetaException) {
        throw (MetaException) rootCause;
      } else {
        throw new MetaException(rootCause.getMessage());
      }
    }

    return baseMetaStoreClient;
  }
}
