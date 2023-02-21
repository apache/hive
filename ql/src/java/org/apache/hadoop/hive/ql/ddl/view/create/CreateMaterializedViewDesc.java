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

package org.apache.hadoop.hive.ql.ddl.view.create;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.ql.ddl.DDLUtils.setColumnsAndStorePartitionTransformSpecOfTable;

/**
 * DDL task description for CREATE VIEW commands.
 */
@Explain(displayName = "Create Materialized View", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class CreateMaterializedViewDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(CreateMaterializedViewDesc.class);

  private String viewName;
  private List<FieldSchema> schema;
  private String comment;
  private Map<String, String> tblProps;
  private List<String> partColNames;
  private boolean ifNotExists;

  private String originalText;
  private String expandedText;
  private boolean rewriteEnabled;
  private List<FieldSchema> partCols;
  private String inputFormat;
  private String outputFormat;
  private String location;
  private String serde;
  private String storageHandler;
  private Map<String, String> serdeProps;
  private Set<TableName> tablesUsed;
  private List<String> sortColNames;
  private List<FieldSchema> sortCols;
  private List<String> distributeColNames;
  private List<FieldSchema> distributeCols;
  private Long initialWriteId; // Initial write ID for CMV and import.
  // The FSOP configuration for the FSOP that is going to write initial data during cmv.
  // This is not needed beyond compilation, so it is transient.
  private transient FileSinkDesc writer;
  private String ownerName = null;

  /**
   * Used to create a materialized view descriptor.
   */
  public CreateMaterializedViewDesc(String viewName, List<FieldSchema> schema, String comment,
      Map<String, String> tblProps, List<String> partColNames, List<String> sortColNames,
      List<String> distributeColNames, boolean ifNotExists, boolean rewriteEnabled,
      String inputFormat, String outputFormat, String location,
      String serde, String storageHandler, Map<String, String> serdeProps) {
    this.viewName = viewName;
    this.schema = schema;
    this.comment = comment;
    this.tblProps = tblProps;
    this.partColNames = partColNames;
    this.sortColNames = sortColNames;
    this.distributeColNames = distributeColNames;
    this.ifNotExists = ifNotExists;

    this.rewriteEnabled = rewriteEnabled;
    this.inputFormat = inputFormat;
    this.outputFormat = outputFormat;
    this.location = location;
    this.serde = serde;
    this.storageHandler = storageHandler;
    this.serdeProps = serdeProps;
  }

  @Explain(displayName = "name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getViewName() {
    return viewName;
  }

  public void setViewName(String viewName) {
    this.viewName = viewName;
  }

  @Explain(displayName = "original text", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getViewOriginalText() {
    return originalText;
  }

  public void setViewOriginalText(String originalText) {
    this.originalText = originalText;
  }

  @Explain(displayName = "expanded text")
  public String getViewExpandedText() {
    return expandedText;
  }

  public void setViewExpandedText(String expandedText) {
    this.expandedText = expandedText;
  }

  @Explain(displayName = "rewrite enabled", displayOnlyOnTrue = true)
  public boolean isRewriteEnabled() {
    return rewriteEnabled;
  }

  public void setRewriteEnabled(boolean rewriteEnabled) {
    this.rewriteEnabled = rewriteEnabled;
  }

  @Explain(displayName = "columns")
  public List<String> getSchemaString() {
    return Utilities.getFieldSchemaString(schema);
  }

  public List<FieldSchema> getSchema() {
    return schema;
  }

  public void setSchema(List<FieldSchema> schema) {
    this.schema = schema;
  }

  @Explain(displayName = "partition columns")
  public List<String> getPartColsString() {
    return Utilities.getFieldSchemaString(partCols);
  }

  public List<FieldSchema> getPartCols() {
    return partCols;
  }

  public void setPartCols(List<FieldSchema> partCols) {
    this.partCols = partCols;
  }

  public List<String> getPartColNames() {
    return partColNames;
  }

  public void setPartColNames(List<String> partColNames) {
    this.partColNames = partColNames;
  }

  public boolean isOrganized() {
    return (sortColNames != null && !sortColNames.isEmpty()) ||
        (distributeColNames != null && !distributeColNames.isEmpty());
  }

  @Explain(displayName = "sort columns")
  public List<String> getSortColsString() {
    return Utilities.getFieldSchemaString(sortCols);
  }

  public List<FieldSchema> getSortCols() {
    return sortCols;
  }

  public void setSortCols(List<FieldSchema> sortCols) {
    this.sortCols = sortCols;
  }

  public List<String> getSortColNames() {
    return sortColNames;
  }

  public void setSortColNames(List<String> sortColNames) {
    this.sortColNames = sortColNames;
  }

  @Explain(displayName = "distribute columns")
  public List<String> getDistributeColsString() {
    return Utilities.getFieldSchemaString(distributeCols);
  }

  public List<FieldSchema> getDistributeCols() {
    return distributeCols;
  }

  public void setDistributeCols(List<FieldSchema> distributeCols) {
    this.distributeCols = distributeCols;
  }

  public List<String> getDistributeColNames() {
    return distributeColNames;
  }

  public void setDistributeColNames(List<String> distributeColNames) {
    this.distributeColNames = distributeColNames;
  }

  @Explain(displayName = "comment")
  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public void setTblProps(Map<String, String> tblProps) {
    this.tblProps = tblProps;
  }

  public Map<String, String> getTblProps() {
    return tblProps;
  }

  @Explain(displayName = "table properties")
  public Map<String, String> getTblPropsExplain() { // only for displaying plan
    return PlanUtils.getPropertiesForExplain(tblProps,
            hive_metastoreConstants.TABLE_IS_CTAS,
            hive_metastoreConstants.TABLE_BUCKETING_VERSION);
  }

  @Explain(displayName = "if not exists", displayOnlyOnTrue = true)
  public boolean getIfNotExists() {
    return ifNotExists;
  }

  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

  public Set<TableName> getTablesUsed() {
    return tablesUsed;
  }

  public void setTablesUsed(Set<TableName> tablesUsed) {
    this.tablesUsed = tablesUsed;
  }

  public String getInputFormat() {
    return inputFormat;
  }

  public void setInputFormat(String inputFormat) {
    this.inputFormat = inputFormat;
  }

  public String getOutputFormat() {
    return outputFormat;
  }

  public void setOutputFormat(String outputFormat) {
    this.outputFormat = outputFormat;
  }

  public void setLocation(String location) {
    this.location = location;
  }
  public String getLocation() {
    return location;
  }

  public String getSerde() {
    return serde;
  }

  public String getStorageHandler() {
    return storageHandler;
  }

  public Map<String, String> getSerdeProps() {
    return serdeProps;
  }

  public Table toTable(HiveConf conf) throws HiveException {
    String[] names = Utilities.getDbTableName(getViewName());
    String databaseName = names[0];
    String tableName = names[1];

    Table tbl = new Table(databaseName, tableName);
    tbl.setViewOriginalText(getViewOriginalText());
    tbl.setViewExpandedText(getViewExpandedText());
    tbl.setRewriteEnabled(isRewriteEnabled());
    tbl.setTableType(TableType.MATERIALIZED_VIEW);
    tbl.setSerializationLib(null);
    tbl.clearSerDeInfo();

    if (getComment() != null) {
      tbl.setProperty("comment", getComment());
    }

    if (tblProps != null) {
      tbl.getParameters().putAll(tblProps);
    }

    if (!CollectionUtils.isEmpty(sortColNames)) {
      tbl.setProperty(Constants.MATERIALIZED_VIEW_SORT_COLUMNS,
          Utilities.encodeColumnNames(sortColNames));
    }
    if (!CollectionUtils.isEmpty(distributeColNames)) {
      tbl.setProperty(Constants.MATERIALIZED_VIEW_DISTRIBUTE_COLUMNS,
          Utilities.encodeColumnNames(distributeColNames));
    }

    if (getInputFormat() != null) {
      tbl.setInputFormatClass(getInputFormat());
    }

    if (getOutputFormat() != null) {
      tbl.setOutputFormatClass(getOutputFormat());
    }

    if (getLocation() != null) {
      tbl.setDataLocation(new Path(getLocation()));
    }

    if (getStorageHandler() != null) {
      tbl.setProperty(
          org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE,
          getStorageHandler());
    }
    if (getSerdeProps() != null) {
      for (Map.Entry<String, String> entry : getSerdeProps().entrySet()) {
        tbl.setSerdeParam(entry.getKey(), entry.getValue());
      }
    }

    HiveStorageHandler storageHandler = tbl.getStorageHandler();

    setColumnsAndStorePartitionTransformSpecOfTable(getSchema(), getPartCols(), conf, tbl);

    /*
     * If the user didn't specify a SerDe, we use the default.
     */
    String serDeClassName;
    if (getSerde() == null) {
      if (storageHandler == null) {
        serDeClassName = PlanUtils.getDefaultSerDe().getName();
        LOG.info("Default to {} for materialized view {}", serDeClassName, getViewName());
      } else {
        serDeClassName = storageHandler.getSerDeClass().getName();
        LOG.info("Use StorageHandler-supplied {} for materialized view {}", serDeClassName, getViewName());
      }
    } else {
      // let's validate that the serde exists
      serDeClassName = getSerde();
      DDLUtils.validateSerDe(serDeClassName, conf);
    }
    tbl.setSerializationLib(serDeClassName);

    // To remain consistent, we need to set input and output formats both
    // at the table level and the storage handler level.
    tbl.setInputFormatClass(getInputFormat());
    tbl.setOutputFormatClass(getOutputFormat());
    if (getInputFormat() != null && !getInputFormat().isEmpty()) {
      tbl.getSd().setInputFormat(tbl.getInputFormatClass().getName());
    }
    if (getOutputFormat() != null && !getOutputFormat().isEmpty()) {
      tbl.getSd().setOutputFormat(tbl.getOutputFormatClass().getName());
    }

    if (ownerName != null) {
      tbl.setOwner(ownerName);
    }

    // Sets the column state for the create view statement (false since it is a creation).
    // Similar to logic in CreateTableDesc.
    StatsSetupConst.setStatsStateForCreateTable(tbl.getTTable().getParameters(), null,
        StatsSetupConst.FALSE);

    return tbl;
  }

  public void setInitialWriteId(Long writeId) {
    this.initialWriteId = writeId;
  }

  public Long getInitialWriteId() {
    return initialWriteId;
  }

  public FileSinkDesc getAndUnsetWriter() {
    FileSinkDesc fsd = writer;
    writer = null;
    return fsd;
  }

  public void setWriter(FileSinkDesc writer) {
    this.writer = writer;
  }

  public void setOwnerName(String ownerName) {
    this.ownerName = ownerName;
  }

  public String getOwnerName() {
    return this.ownerName;
  }

}
