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
package org.apache.hadoop.hive.metastore;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.metastore.api.InitializeTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.events.PreAlterTableEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateTableEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.HiveStrictManagedUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TransactionalValidationListener extends MetaStorePreEventListener {
  public static final Logger LOG = LoggerFactory.getLogger(TransactionalValidationListener.class);

  // These constants are also imported by org.apache.hadoop.hive.ql.io.AcidUtils.
  public static final String DEFAULT_TRANSACTIONAL_PROPERTY = "default";
  public static final String INSERTONLY_TRANSACTIONAL_PROPERTY = "insert_only";

  private final Set<String> supportedCatalogs = new HashSet<String>();

  TransactionalValidationListener(Configuration conf) {
    super(conf);
    supportedCatalogs.add("hive");
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
    if (supportedCatalogs.contains(getTableCatalog(context.getNewTable()))) {
      handleAlterTableTransactionalProp(context);
      HiveStrictManagedUtils.validateStrictManagedTableWithThrow(getConf(), context.getNewTable());
    }
  }

  private void handle(PreCreateTableEvent context) throws MetaException {
    if (supportedCatalogs.contains(getTableCatalog(context.getTable()))) {
      handleCreateTableTransactionalProp(context);
      HiveStrictManagedUtils.validateStrictManagedTableWithThrow(getConf(), context.getTable());
    }
  }

  private String getTableCatalog(Table table) {
    String catName = table.isSetCatName() ? table.getCatName() :
      MetaStoreUtils.getDefaultCatalog(getConf());
    return catName.toLowerCase();
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

    if (transactionalValuePresent && "false".equalsIgnoreCase(transactionalValue)) {
      transactionalValuePresent = false;
      transactionalValue = null;
    }

    if (transactionalValuePresent) {
      //normalize prop name
      parameters.put(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, transactionalValue);
    }
    if ("true".equalsIgnoreCase(transactionalValue) && !"true".equalsIgnoreCase(oldTransactionalValue)) {
      if(!isTransactionalPropertiesPresent) {
        normalizeTransactionalPropertyDefault(newTable);
        isTransactionalPropertiesPresent = true;
        transactionalPropertiesValue = DEFAULT_TRANSACTIONAL_PROPERTY;
      }
      // We only need to check conformance if alter table enabled acid.
      // INSERT_ONLY tables don't have to conform to ACID requirement like ORC or bucketing.
      boolean isFullAcid = transactionalPropertiesValue == null
          || !"insert_only".equalsIgnoreCase(transactionalPropertiesValue);
      if (isFullAcid && !conformToAcid(newTable)) {
        throw new MetaException("The table must be stored using an ACID compliant "
            + "format (such as ORC): " + Warehouse.getQualifiedName(newTable));
      }

      if (newTable.getTableType().equals(TableType.EXTERNAL_TABLE.toString())) {
        throw new MetaException(Warehouse.getQualifiedName(newTable) +
            " cannot be declared transactional because it's an external table");
      }
      if (isFullAcid) {
        validateTableStructure(context.getHandler(), newTable);
      }
      hasValidTransactionalValue = true;
    }



    if (oldTransactionalValue == null ? transactionalValue == null
                                     : oldTransactionalValue.equalsIgnoreCase(transactionalValue)) {
      //this covers backward compat cases where this prop may have been set already
      hasValidTransactionalValue = true;
    }

    if (!hasValidTransactionalValue && !MetaStoreUtils.isInsertOnlyTableParam(oldTable.getParameters())) {
      // if here, there is attempt to set transactional to something other than 'true'
      // and NOT the same value it was before
      throw new MetaException("TBLPROPERTIES with 'transactional'='true' cannot be unset: "
          + Warehouse.getQualifiedName(newTable));
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
        if ((oldTransactionalPropertiesValue == null
            || !oldTransactionalPropertiesValue.equalsIgnoreCase(transactionalPropertiesValue))
            && !MetaStoreUtils.isInsertOnlyTableParam(oldTable.getParameters())) {
          throw new MetaException("TBLPROPERTIES with 'transactional_properties' cannot be "
              + "altered after the table is created");
        }
      }
    }
    checkSorted(newTable);
    if(TxnUtils.isAcidTable(newTable) && !TxnUtils.isAcidTable(oldTable)) {
      /* we just made an existing table full acid which wasn't acid before and it passed all checks
      initialize the Write ID sequence so that we can handle assigning ROW_IDs to 'original'
      files already present in the table. */
      TxnStore t = TxnUtils.getTxnStore(getConf());
      //For now assume no partition may have > 10M files.  Perhaps better to count them.
      t.seedWriteIdOnAcidConversion(new InitializeTableWriteIdsRequest(newTable.getDbName(),
          newTable.getTableName(), 10000000));
    }
  }

  private void checkSorted(Table newTable) throws MetaException {
    if(!TxnUtils.isAcidTable(newTable)) {
      return;
    }
    StorageDescriptor sd = newTable.getSd();
    if (sd.getSortCols() != null && sd.getSortCols().size() > 0) {
      throw new MetaException("Table " + Warehouse.getQualifiedName(newTable)
        + " cannot support full ACID functionality since it is sorted.");
    }
  }

  /**
   * Want to make a a newly create table Acid (unless it explicitly has transactional=false param)
   * if table can support it.  Also see SemanticAnalyzer.addDefaultProperties() which performs the
   * same logic.  This code path is more general since it is activated even if you create a table
   * via Thrift, WebHCat etc but some operations like CTAS create the table (metastore object) as
   * the last step (i.e. after the data is written) but write itself is has to be aware of the type
   * of table so this Listener is too late.
   */
  private void makeAcid(Table newTable) throws MetaException {
    if(newTable.getParameters() != null &&
        newTable.getParameters().containsKey(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL)) {
      LOG.info("Could not make " + Warehouse.getQualifiedName(newTable) + " acid: already has " +
          hive_metastoreConstants.TABLE_IS_TRANSACTIONAL + "=" +
          newTable.getParameters().get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL));
      return;
    }

    Configuration conf = getConf();
    boolean makeAcid =
        //no point making an acid table if these other props are not set since it will just throw
        //exceptions when someone tries to use the table.
        MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.CREATE_TABLES_AS_ACID) &&
        MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.HIVE_SUPPORT_CONCURRENCY) &&
        "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager".equals(
            MetastoreConf.getVar(conf, MetastoreConf.ConfVars.HIVE_TXN_MANAGER)
        );

    if(makeAcid) {
      if(!conformToAcid(newTable)) {
        LOG.info("Could not make " + Warehouse.getQualifiedName(newTable) + " acid: wrong IO format");
        return;
      }
      if(!TableType.MANAGED_TABLE.toString().equalsIgnoreCase(newTable.getTableType())) {
        //todo should this check be in conformToAcid()?
        LOG.info("Could not make " + Warehouse.getQualifiedName(newTable) + " acid: it's " +
            newTable.getTableType());
        return;
      }
      if(newTable.getSd().getSortColsSize() > 0) {
        LOG.info("Could not make " + Warehouse.getQualifiedName(newTable) + " acid: it's sorted");
        return;
      }
      //check if orc and not sorted
      Map<String, String> parameters = newTable.getParameters();
      if (parameters == null || parameters.isEmpty()) {
        parameters = new HashMap<>();
      }
      parameters.put(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, "true");
      newTable.setParameters(parameters);
      LOG.info("Automatically chose to make " + Warehouse.getQualifiedName(newTable) + " acid.");
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
      makeAcid(newTable);
      return;
    }
    String transactional = null;
    String transactionalProperties = null;
    Set<String> keys = new HashSet<>(parameters.keySet());
    for(String key : keys) {
      // Get the "transactional" tblproperties value
      if (hive_metastoreConstants.TABLE_IS_TRANSACTIONAL.equalsIgnoreCase(key)) {
        transactional = parameters.get(key);
        parameters.remove(key);
      }

      // Get the "transactional_properties" tblproperties value
      if (hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES.equalsIgnoreCase(key)) {
        transactionalProperties = parameters.get(key);
      }
    }

    if (transactional == null) {
      makeAcid(newTable);
      return;
    }

    if ("false".equalsIgnoreCase(transactional)) {
      // just drop transactional=false - absence of 'transactional' property is equivalent to
      // transactional=false
      return;
    }

    if ("true".equalsIgnoreCase(transactional)) {
      if (!conformToAcid(newTable)) {
        // INSERT_ONLY tables don't have to conform to ACID requirement like ORC or bucketing
        if (transactionalProperties == null || !"insert_only".equalsIgnoreCase(transactionalProperties)) {
          throw new MetaException("The table must be stored using an ACID compliant format (such as ORC): "
              + Warehouse.getQualifiedName(newTable));
        }
      }

      if (MetaStoreUtils.isExternalTable(newTable)) {
        throw new MetaException(Warehouse.getQualifiedName(newTable) +
            " cannot be declared transactional because it's an external table");
      }

      // normalize prop name
      parameters.put(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, Boolean.TRUE.toString());
      if(transactionalProperties == null) {
        normalizeTransactionalPropertyDefault(newTable);
      }
      initializeTransactionalProperties(newTable);
      checkSorted(newTable);
      return;
    }
    // transactional is found, but the value is not in expected range
    throw new MetaException("'transactional' property of TBLPROPERTIES may only have value 'true': "
        + Warehouse.getQualifiedName(newTable));
  }

  /**
   * When a table is marked transactional=true but transactional_properties is not set then
   * transactional_properties should take on the default value.  Easier to make this explicit in
   * table definition than keep checking everywhere if it's set or not.
   */
  private void normalizeTransactionalPropertyDefault(Table table) {
    table.getParameters().put(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES,
        DEFAULT_TRANSACTIONAL_PROPERTY);

  }
  /**
   * Check that InputFormatClass/OutputFormatClass should implement
   * AcidInputFormat/AcidOutputFormat
   */
  public static boolean conformToAcid(Table table) throws MetaException {
    StorageDescriptor sd = table.getSd();
    try {
      Class inputFormatClass = sd.getInputFormat() == null ? null :
          Class.forName(sd.getInputFormat());
      Class outputFormatClass = sd.getOutputFormat() == null ? null :
          Class.forName(sd.getOutputFormat());

      if (inputFormatClass == null || outputFormatClass == null ||
          !Class.forName("org.apache.hadoop.hive.ql.io.AcidInputFormat").isAssignableFrom(inputFormatClass) ||
          !Class.forName("org.apache.hadoop.hive.ql.io.AcidOutputFormat").isAssignableFrom(outputFormatClass)) {
        return false;
      }
    } catch (ClassNotFoundException e) {
      LOG.warn("Could not verify InputFormat=" + sd.getInputFormat() + " or OutputFormat=" +
          sd.getOutputFormat() + "  for " + Warehouse.getQualifiedName(table));
      return false;
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
            throw new MetaException("Invalid transactional properties specified for "
                + Warehouse.getQualifiedName(table) + " with the error " + validationError);
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
      case INSERTONLY_TRANSACTIONAL_PROPERTY:
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
  private final Pattern ORIGINAL_PATTERN = Pattern.compile("[0-9]+_[0-9]+");
  /**
   * see org.apache.hadoop.hive.ql.exec.Utilities#COPY_KEYWORD
   */
  private static final Pattern ORIGINAL_PATTERN_COPY =
    Pattern.compile("[0-9]+_[0-9]+" + "_copy_" + "[0-9]+");

  /**
   * It's assumed everywhere that original data files are named according to
   * {@link #ORIGINAL_PATTERN} or{@link #ORIGINAL_PATTERN_COPY}
   * This checks that when transaction=true is set and throws if it finds any files that don't
   * follow convention.
   */
  private void validateTableStructure(IHMSHandler hmsHandler, Table table)
    throws MetaException {
    Path tablePath;
    try {
      Warehouse wh = hmsHandler.getWh();
      if (table.getSd().getLocation() == null || table.getSd().getLocation().isEmpty()) {
        String catName = getTableCatalog(table);
        tablePath = wh.getDefaultTablePath(hmsHandler.getMS().getDatabase(
            catName, table.getDbName()), table);
      } else {
        tablePath = wh.getDnsPath(new Path(table.getSd().getLocation()));
      }
      FileSystem fs = wh.getFs(tablePath);
      //FileSystem fs = FileSystem.get(getConf());
      RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(tablePath, true);
      while (iterator.hasNext()) {
        LocatedFileStatus fileStatus = iterator.next();
        if (!fileStatus.isFile()) {
          continue;
        }
        boolean validFile =
          (ORIGINAL_PATTERN.matcher(fileStatus.getPath().getName()).matches() ||
            ORIGINAL_PATTERN_COPY.matcher(fileStatus.getPath().getName()).matches()
          );
        if (!validFile) {
          throw new IllegalStateException("Unexpected data file name format.  Cannot convert " +
            Warehouse.getQualifiedName(table) + " to transactional table.  File: "
            + fileStatus.getPath());
        }
      }
    } catch (IOException|NoSuchObjectException e) {
      String msg = "Unable to list files for " + Warehouse.getQualifiedName(table);
      LOG.error(msg, e);
      MetaException e1 = new MetaException(msg);
      e1.initCause(e);
      throw e1;
    }
  }
}
