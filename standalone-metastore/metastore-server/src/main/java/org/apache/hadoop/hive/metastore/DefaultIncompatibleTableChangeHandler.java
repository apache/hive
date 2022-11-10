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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default incompatible table change handler. This is invoked by the {@link
 * HiveAlterHandler} when a table is altered to check if the column type changes if any
 * are allowed or not.
 */
public class DefaultIncompatibleTableChangeHandler implements
    IMetaStoreIncompatibleChangeHandler {

  private static final Logger LOG = LoggerFactory
      .getLogger(DefaultIncompatibleTableChangeHandler.class);
  private static final DefaultIncompatibleTableChangeHandler INSTANCE =
      new DefaultIncompatibleTableChangeHandler();

  private DefaultIncompatibleTableChangeHandler() {
  }

  public static DefaultIncompatibleTableChangeHandler get() {
    return INSTANCE;
  }

  /**
   * Checks if the column type changes in the oldTable and newTable are allowed or not. In
   * addition to checking if the incompatible changes are allowed or not, this also checks
   * if the table serde library belongs to a list of table serdes which support making any
   * column type changes.
   *
   * @param conf     The configuration which if incompatible col type changes are allowed
   *                 or not.
   * @param oldTable The instance of the table being altered.
   * @param newTable The new instance of the table which represents the altered state of
   *                 the table.
   * @throws InvalidOperationException
   */
  @Override
  public void allowChange(Configuration conf, Table oldTable, Table newTable)
      throws InvalidOperationException {
    if (!MetastoreConf.getBoolVar(conf,
        MetastoreConf.ConfVars.DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES)) {
      // incompatible column changes are allowed for all
      return;
    }
    if (oldTable.getTableType().equals(TableType.VIRTUAL_VIEW.toString())) {
      // Views derive the column type from the base table definition. So the view
      // definition can be altered to change the column types. The column type
      // compatibility checks should be done only for non-views.
      return;
    }
    checkColTypeChangeCompatible(conf, oldTable, newTable);
  }

  private void checkColTypeChangeCompatible(Configuration conf, Table oldTable,
      Table newTable) throws InvalidOperationException {
    List<FieldSchema> oldCols = oldTable.getSd().getCols();
    List<FieldSchema> newCols = newTable.getSd().getCols();
    List<String> incompatibleCols = new ArrayList<>();
    int maxCols = Math.min(oldCols.size(), newCols.size());
    for (int i = 0; i < maxCols; i++) {
      if (!ColumnType.areColTypesCompatible(
          ColumnType.getTypeName(oldCols.get(i).getType()),
          ColumnType.getTypeName(newCols.get(i).getType()))) {
        incompatibleCols.add(newCols.get(i).getName());
      }
    }
    if (!incompatibleCols.isEmpty()) {
      Collection<String> exceptedTableSerdes = MetastoreConf.getStringCollection(conf,
          MetastoreConf.ConfVars.ALLOW_INCOMPATIBLE_COL_TYPE_CHANGES_TABLE_SERDES);
      SerDeInfo serDeInfo = oldTable.getSd().getSerdeInfo();
      String serializationLib =
          serDeInfo == null ? null : serDeInfo.getSerializationLib();
      if (exceptedTableSerdes.contains(serializationLib)) {
        LOG.info(
            "Allowing incompatible column type change of {} for table {}"
                + " since the table serde {} is in excepted list of serdes",
            incompatibleCols, (oldTable.getDbName() + "." + oldTable.getTableName()),
            serializationLib);
        return;
      }
      throw new InvalidOperationException(
          "The following columns have types incompatible with the existing " +
              "columns in their respective positions :\n" +
              org.apache.commons.lang3.StringUtils.join(incompatibleCols, ',')
      );
    }
  }
}
