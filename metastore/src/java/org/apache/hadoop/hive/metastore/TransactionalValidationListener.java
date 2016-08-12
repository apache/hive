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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.events.PreAlterTableEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateTableEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TransactionalValidationListener extends MetaStorePreEventListener {
  public static final Logger LOG = LoggerFactory.getLogger(TransactionalValidationListener.class);

  // These constants are also imported by org.apache.hadoop.hive.ql.io.AcidUtils.
  public static final String DEFAULT_TRANSACTIONAL_PROPERTY = "default";
  public static final String LEGACY_TRANSACTIONAL_PROPERTY = "legacy";

  TransactionalValidationListener(Configuration conf) {
    super(conf);
  }

  @Override
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
   * Also in current version, 'transactional_properties' of the table cannot be altered after
   * the table is created. Any attempt to alter it will throw a MetaException.
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
    boolean isTransactionalPropertiesPresent = false;
    String transactionalPropertiesValue = null;
    boolean hasValidTransactionalValue = false;

    for (String key : keys) {
      if(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL.equalsIgnoreCase(key)) {
        transactionalValuePresent = true;
        transactionalValue = parameters.get(key);
        parameters.remove(key);
      }
      if(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES.equalsIgnoreCase(key)) {
        isTransactionalPropertiesPresent = true;
        transactionalPropertiesValue = parameters.get(key);
        // Do not remove the parameter yet, because we have separate initialization routine
        // that will use it down below.
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
      hasValidTransactionalValue = true;
    }

    Table oldTable = context.getOldTable();
    String oldTransactionalValue = null;
    String oldTransactionalPropertiesValue = null;
    for (String key : oldTable.getParameters().keySet()) {
      if (hive_metastoreConstants.TABLE_IS_TRANSACTIONAL.equalsIgnoreCase(key)) {
        oldTransactionalValue = oldTable.getParameters().get(key);
      }
      if (hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES.equalsIgnoreCase(key)) {
        oldTransactionalPropertiesValue = oldTable.getParameters().get(key);
      }
    }


    if (oldTransactionalValue == null ? transactionalValue == null
                                     : oldTransactionalValue.equalsIgnoreCase(transactionalValue)) {
      //this covers backward compat cases where this prop may have been set already
      hasValidTransactionalValue = true;
    }

    if (!hasValidTransactionalValue) {
      // if here, there is attempt to set transactional to something other than 'true'
      // and NOT the same value it was before
      throw new MetaException("TBLPROPERTIES with 'transactional'='true' cannot be unset");
    }

    if (isTransactionalPropertiesPresent) {
      // Now validate transactional_properties for the table.
      if (oldTransactionalValue == null) {
        // If this is the first time the table is being initialized to 'transactional=true',
        // any valid value can be set for the 'transactional_properties'.
        initializeTransactionalProperties(newTable);
      } else {
        // If the table was already marked as 'transactional=true', then the new value of
        // 'transactional_properties' must match the old value. Any attempt to alter the previous
        // value will throw an error. An exception will still be thrown if the previous value was
        // null and an attempt is made to set it. This behaviour can be changed in the future.
        if (oldTransactionalPropertiesValue == null
            || !oldTransactionalPropertiesValue.equalsIgnoreCase(transactionalPropertiesValue) ) {
          throw new MetaException("TBLPROPERTIES with 'transactional_properties' cannot be "
              + "altered after the table is created");
        }
      }
    }
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
      initializeTransactionalProperties(newTable);
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

  private void initializeTransactionalProperties(Table table) throws MetaException {
    // All new versions of Acid tables created after the introduction of Acid version/type system
    // can have TRANSACTIONAL_PROPERTIES property defined. This parameter can be used to change
    // the operational behavior of ACID. However if this parameter is not defined, the new Acid
    // tables will still behave as the old ones. This is done so to preserve the behavior
    // in case of rolling downgrade.

    // Initialize transaction table properties with default string value.
    String tableTransactionalProperties = null;

    Map<String, String> parameters = table.getParameters();
    if (parameters != null) {
      Set<String> keys = parameters.keySet();
      for (String key : keys) {
        if (hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES.equalsIgnoreCase(key)) {
          tableTransactionalProperties = parameters.get(key).toLowerCase();
          parameters.remove(key);
          String validationError = validateTransactionalProperties(tableTransactionalProperties);
          if (validationError != null) {
            throw new MetaException("Invalid transactional properties specified for the "
                + "table with the error " + validationError);
          }
          break;
        }
      }
    }

    if (tableTransactionalProperties != null) {
      parameters.put(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES,
              tableTransactionalProperties);
    }
  }

  private String validateTransactionalProperties(String transactionalProperties) {
    boolean isValid = false;
    switch (transactionalProperties) {
      case DEFAULT_TRANSACTIONAL_PROPERTY:
      case LEGACY_TRANSACTIONAL_PROPERTY:
        isValid = true;
        break;
      default:
        isValid = false;
    }
    if (!isValid) {
      return "unknown value " + transactionalProperties +  " for transactional_properties";
    }
    return null; // All checks passed, return null.
  }
}
