/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.StringUtils;

import java.util.List;

public class SerDeStorageSchemaReader implements StorageSchemaReader {
  @Override
  public List<FieldSchema> readSchema(Table tbl, EnvironmentContext envContext, Configuration conf)
     throws MetaException {
    ClassLoader orgHiveLoader = null;
    try {
      if (envContext != null) {
        String addedJars = envContext.getProperties().get("hive.added.jars.path");
        if (org.apache.commons.lang3.StringUtils.isNotBlank(addedJars)) {
          //for thread safe
          orgHiveLoader = conf.getClassLoader();
          ClassLoader loader = org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.addToClassPath(
              orgHiveLoader, org.apache.commons.lang3.StringUtils.split(addedJars, ","));
          conf.setClassLoader(loader);
        }
      }

      Deserializer s = HiveMetaStoreUtils.getDeserializer(conf, tbl, false);
      return HiveMetaStoreUtils.getFieldsFromDeserializer(tbl.getTableName(), s);
    } catch (Exception e) {
      StringUtils.stringifyException(e);
      throw new MetaException(e.getMessage());
    } finally {
      if (orgHiveLoader != null) {
        conf.setClassLoader(orgHiveLoader);
      }
    }
  }
}
