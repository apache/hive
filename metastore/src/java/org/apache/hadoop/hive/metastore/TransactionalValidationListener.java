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
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.events.PreAlterTableEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateTableEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

final class TransactionalValidationListener extends MetaStorePreEventListener {
  public static final Logger LOG = LoggerFactory.getLogger(TransactionalValidationListener.class);

  TransactionalValidationListener(Configuration conf) {
    super(conf);
  }

  public void onEvent(PreEventContext context) throws MetaException, NoSuchObjectException,
      InvalidOperationException {
    switch (context.getEventType()) {
      case CREATE_TABLE:
        handle((PreCreateTableEvent) context);
        break;
      case ALTER_TABLE:
        handle((PreAlterTableEvent) context);
        break;
      default:
        //no validation required..
    }
  }

  private void handle(PreAlterTableEvent context) throws MetaException {
    handleAlterTableTransactionalProp(context);
  }

  private void handle(PreCreateTableEvent context) throws MetaException {
    handleCreateTableTransactionalProp(context);
  }

  /**
   * once a table is marked transactional, you cannot go back.  Enforce this.
   */
  private void handleAlterTableTransactionalProp(PreAlterTableEvent context) throws MetaException {
    Table newTable = context.getNewTable();
    Map<String, String> parameters = newTable.getParameters();
    if (parameters == null || parameters.isEmpty()) {
      return;
    }
    Set<String> keys = new HashSet<>(parameters.keySet());
    String transactionalValue = null;
    boolean transactionalValuePresent = false;
    for (String key : keys) {
      if(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL.equalsIgnoreCase(key)) {
        transactionalValuePresent = true;
        transactionalValue = parameters.get(key);
        parameters.remove(key);
      }
    }
    if (transactionalValuePresent) {
      //normalize prop name
      parameters.put(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, transactionalValue);
    }
    if ("true".equalsIgnoreCase(transactionalValue)) {
      if (!conformToAcid(newTable)) {
        throw new MetaException("The table must be bucketed and stored using an ACID compliant" +
            " format (such as ORC)");
      }

      if (newTable.getTableType().equals(TableType.EXTERNAL_TABLE.toString())) {
        throw new MetaException(newTable.getDbName() + "." + newTable.getTableName() +
            " cannot be declared transactional because it's an external table");
      }

      return;
    }
    Table oldTable = context.getOldTable();
    String oldTransactionalValue = null;
    for (String key : oldTable.getParameters().keySet()) {
      if (hive_metastoreConstants.TABLE_IS_TRANSACTIONAL.equalsIgnoreCase(key)) {
        oldTransactionalValue = oldTable.getParameters().get(key);
      }
    }
    if (oldTransactionalValue == null ? transactionalValue == null
                                     : oldTransactionalValue.equalsIgnoreCase(transactionalValue)) {
      //this covers backward compat cases where this prop may have been set already
      return;
    }
    // if here, there is attempt to set transactional to something other than 'true'
    // and NOT the same value it was before
    throw new MetaException("TBLPROPERTIES with 'transactional'='true' cannot be unset");
  }

  /**
   * Normalize case and make sure:
   * 1. 'true' is the only value to be set for 'transactional' (if set at all)
   * 2. If set to 'true', we should also enforce bucketing and ORC format
   */
  private void handleCreateTableTransactionalProp(PreCreateTableEvent context) throws MetaException {
    Table newTable = context.getTable();
    Map<String, String> parameters = newTable.getParameters();
    if (parameters == null || parameters.isEmpty()) {
      return;
    }
    String transactionalValue = null;
    boolean transactionalPropFound = false;
    Set<String> keys = new HashSet<>(parameters.keySet());
    for(String key : keys) {
      if(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL.equalsIgnoreCase(key)) {
        transactionalPropFound = true;
        transactionalValue = parameters.get(key);
        parameters.remove(key);
      }
    }

    if (!transactionalPropFound) {
      return;
    }

    if ("false".equalsIgnoreCase(transactionalValue)) {
      // just drop transactional=false.  For backward compatibility in case someone has scripts
      // with transactional=false
      LOG.info("'transactional'='false' is no longer a valid property and will be ignored");
      return;
    }

    if ("true".equalsIgnoreCase(transactionalValue)) {
      if (!conformToAcid(newTable)) {
        throw new MetaException("The table must be bucketed and stored using an ACID compliant" +
            " format (such as ORC)");
      }

      if (newTable.getTableType().equals(TableType.EXTERNAL_TABLE.toString())) {
        throw new MetaException(newTable.getDbName() + "." + newTable.getTableName() +
            " cannot be declared transactional because it's an external table");
      }

      // normalize prop name
      parameters.put(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, Boolean.TRUE.toString());
      return;
    }

    // transactional prop is found, but the value is not in expected range
    throw new MetaException("'transactional' property of TBLPROPERTIES may only have value 'true'");
  }

  // Check if table is bucketed and InputFormatClass/OutputFormatClass should implement
  // AcidInputFormat/AcidOutputFormat
  private boolean conformToAcid(Table table) throws MetaException {
    StorageDescriptor sd = table.getSd();
    if (sd.getBucketColsSize() < 1) {
      return false;
    }

    try {
      Class inputFormatClass = Class.forName(sd.getInputFormat());
      Class outputFormatClass = Class.forName(sd.getOutputFormat());

      if (inputFormatClass == null || outputFormatClass == null ||
          !Class.forName("org.apache.hadoop.hive.ql.io.AcidInputFormat").isAssignableFrom(inputFormatClass) ||
          !Class.forName("org.apache.hadoop.hive.ql.io.AcidOutputFormat").isAssignableFrom(outputFormatClass)) {
        return false;
      }
    } catch (ClassNotFoundException e) {
      throw new MetaException("Invalid input/output format for table");
    }

    return true;
  }
}