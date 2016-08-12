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
package org.apache.hadoop.hive.druid;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.druid.serde.DruidSerDe;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DruidStorageHandler provides a HiveStorageHandler implementation for Druid.
 */
@SuppressWarnings({"deprecation","rawtypes"})
public class DruidStorageHandler extends DefaultStorageHandler implements HiveMetaHook {

  protected static final Logger LOG = LoggerFactory.getLogger(DruidStorageHandler.class);

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return HiveDruidQueryBasedInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return HiveDruidOutputFormat.class;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return DruidSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return this;
  }

  @Override
  public void preCreateTable(Table table) throws MetaException {
    // Do safety checks
    if (!MetaStoreUtils.isExternalTable(table)) {
      throw new MetaException("Table in Druid needs to be declared as EXTERNAL");
    }
    if (!StringUtils.isEmpty(table.getSd().getLocation())) {
      throw new MetaException("LOCATION may not be specified for Druid");
    }
    if (table.getPartitionKeysSize() != 0) {
      throw new MetaException("PARTITIONED BY may not be specified for Druid");
    }
    if (table.getSd().getBucketColsSize() != 0) {
      throw new MetaException("CLUSTERED BY may not be specified for Druid");
    }
  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
    // Nothing to do
  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {
    // Nothing to do
  }

  @Override
  public void preDropTable(Table table) throws MetaException {
    // Nothing to do
  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {
    // Nothing to do
  }

  @Override
  public void commitDropTable(Table table, boolean deleteData) throws MetaException {
    // Nothing to do
  }

  @Override
  public String toString() {
    return Constants.DRUID_HIVE_STORAGE_HANDLER_ID;
  }

}
