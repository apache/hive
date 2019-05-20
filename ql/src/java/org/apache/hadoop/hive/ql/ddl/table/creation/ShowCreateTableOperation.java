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

package org.apache.hadoop.hive.ql.ddl.table.creation;

import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hive.common.util.HiveStringUtils;
import org.stringtemplate.v4.ST;

/**
 * Operation process showing the creation of a table.
 */
public class ShowCreateTableOperation extends DDLOperation {
  private static final String EXTERNAL = "external";
  private static final String TEMPORARY = "temporary";
  private static final String LIST_COLUMNS = "columns";
  private static final String TBL_COMMENT = "tbl_comment";
  private static final String LIST_PARTITIONS = "partitions";
  private static final String SORT_BUCKET = "sort_bucket";
  private static final String SKEWED_INFO = "tbl_skewedinfo";
  private static final String ROW_FORMAT = "row_format";
  private static final String TBL_LOCATION = "tbl_location";
  private static final String TBL_PROPERTIES = "tbl_properties";

  private final ShowCreateTableDesc desc;

  public ShowCreateTableOperation(DDLOperationContext context, ShowCreateTableDesc desc) {
    super(context);
    this.desc = desc;
  }

  @Override
  public int execute() throws HiveException {
    // get the create table statement for the table and populate the output
    try (DataOutputStream outStream = DDLUtils.getOutputStream(new Path(desc.getResFile()), context)) {
      return showCreateTable(outStream);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  private int showCreateTable(DataOutputStream outStream) throws HiveException {
    boolean needsLocation = true;
    StringBuilder createTabCommand = new StringBuilder();

    Table tbl = context.getDb().getTable(desc.getTableName(), false);
    List<String> duplicateProps = new ArrayList<String>();
    try {
      needsLocation = CreateTableOperation.doesTableNeedLocation(tbl);

      if (tbl.isView()) {
        String createTabStmt = "CREATE VIEW `" + desc.getTableName() + "` AS " + tbl.getViewExpandedText();
        outStream.write(createTabStmt.getBytes(StandardCharsets.UTF_8));
        return 0;
      }

      createTabCommand.append("CREATE <" + TEMPORARY + "><" + EXTERNAL + ">TABLE `");
      createTabCommand.append(desc.getTableName() + "`(\n");
      createTabCommand.append("<" + LIST_COLUMNS + ">)\n");
      createTabCommand.append("<" + TBL_COMMENT + ">\n");
      createTabCommand.append("<" + LIST_PARTITIONS + ">\n");
      createTabCommand.append("<" + SORT_BUCKET + ">\n");
      createTabCommand.append("<" + SKEWED_INFO + ">\n");
      createTabCommand.append("<" + ROW_FORMAT + ">\n");
      if (needsLocation) {
        createTabCommand.append("LOCATION\n");
        createTabCommand.append("<" + TBL_LOCATION + ">\n");
      }
      createTabCommand.append("TBLPROPERTIES (\n");
      createTabCommand.append("<" + TBL_PROPERTIES + ">)\n");
      ST createTabStmt = new ST(createTabCommand.toString());

      // For cases where the table is temporary
      String tblTemp = "";
      if (tbl.isTemporary()) {
        duplicateProps.add("TEMPORARY");
        tblTemp = "TEMPORARY ";
      }
      // For cases where the table is external
      String tblExternal = "";
      if (tbl.getTableType() == TableType.EXTERNAL_TABLE) {
        duplicateProps.add("EXTERNAL");
        tblExternal = "EXTERNAL ";
      }

      // Columns
      String tblColumns = "";
      List<FieldSchema> cols = tbl.getCols();
      List<String> columns = new ArrayList<String>();
      for (FieldSchema col : cols) {
        String columnDesc = "  `" + col.getName() + "` " + col.getType();
        if (col.getComment() != null) {
          columnDesc = columnDesc + " COMMENT '" + HiveStringUtils.escapeHiveCommand(col.getComment()) + "'";
        }
        columns.add(columnDesc);
      }
      tblColumns = StringUtils.join(columns, ", \n");

      // Table comment
      String tblComment = "";
      String tabComment = tbl.getProperty("comment");
      if (tabComment != null) {
        duplicateProps.add("comment");
        tblComment = "COMMENT '" + HiveStringUtils.escapeHiveCommand(tabComment) + "'";
      }

      // Partitions
      String tblPartitions = "";
      List<FieldSchema> partKeys = tbl.getPartitionKeys();
      if (partKeys.size() > 0) {
        tblPartitions += "PARTITIONED BY ( \n";
        List<String> partCols = new ArrayList<String>();
        for (FieldSchema partKey : partKeys) {
          String partColDesc = "  `" + partKey.getName() + "` " + partKey.getType();
          if (partKey.getComment() != null) {
            partColDesc = partColDesc + " COMMENT '" + HiveStringUtils.escapeHiveCommand(partKey.getComment()) + "'";
          }
          partCols.add(partColDesc);
        }
        tblPartitions += StringUtils.join(partCols, ", \n");
        tblPartitions += ")";
      }

      // Clusters (Buckets)
      String tblSortBucket = "";
      List<String> buckCols = tbl.getBucketCols();
      if (buckCols.size() > 0) {
        duplicateProps.add("SORTBUCKETCOLSPREFIX");
        tblSortBucket += "CLUSTERED BY ( \n  ";
        tblSortBucket += StringUtils.join(buckCols, ", \n  ");
        tblSortBucket += ") \n";
        List<Order> sortCols = tbl.getSortCols();
        if (sortCols.size() > 0) {
          tblSortBucket += "SORTED BY ( \n";
          // Order
          List<String> sortKeys = new ArrayList<String>();
          for (Order sortCol : sortCols) {
            String sortKeyDesc = "  " + sortCol.getCol() + " ";
            if (sortCol.getOrder() == BaseSemanticAnalyzer.HIVE_COLUMN_ORDER_ASC) {
              sortKeyDesc = sortKeyDesc + "ASC";
            } else if (sortCol.getOrder() == BaseSemanticAnalyzer.HIVE_COLUMN_ORDER_DESC) {
              sortKeyDesc = sortKeyDesc + "DESC";
            }
            sortKeys.add(sortKeyDesc);
          }
          tblSortBucket += StringUtils.join(sortKeys, ", \n");
          tblSortBucket += ") \n";
        }
        tblSortBucket += "INTO " + tbl.getNumBuckets() + " BUCKETS";
      }

      // Skewed Info
      StringBuilder tblSkewedInfo = new StringBuilder();
      SkewedInfo skewedInfo = tbl.getSkewedInfo();
      if (skewedInfo != null && !skewedInfo.getSkewedColNames().isEmpty()) {
        tblSkewedInfo.append("SKEWED BY (" + StringUtils.join(skewedInfo.getSkewedColNames(), ",") + ")\n");
        tblSkewedInfo.append("  ON (");
        List<String> colValueList = new ArrayList<String>();
        for (List<String> colValues : skewedInfo.getSkewedColValues()) {
          colValueList.add("('" + StringUtils.join(colValues, "','") + "')");
        }
        tblSkewedInfo.append(StringUtils.join(colValueList, ",") + ")");
        if (tbl.isStoredAsSubDirectories()) {
          tblSkewedInfo.append("\n  STORED AS DIRECTORIES");
        }
      }

      // Row format (SerDe)
      StringBuilder tblRowFormat = new StringBuilder();
      StorageDescriptor sd = tbl.getTTable().getSd();
      SerDeInfo serdeInfo = sd.getSerdeInfo();
      Map<String, String> serdeParams = serdeInfo.getParameters();
      tblRowFormat.append("ROW FORMAT SERDE \n");
      tblRowFormat.append("  '" + HiveStringUtils.escapeHiveCommand(serdeInfo.getSerializationLib()) + "' \n");
      if (tbl.getStorageHandler() == null) {
        // If serialization.format property has the default value, it will not to be included in
        // SERDE properties
        if (Warehouse.DEFAULT_SERIALIZATION_FORMAT.equals(serdeParams.get(serdeConstants.SERIALIZATION_FORMAT))) {
          serdeParams.remove(serdeConstants.SERIALIZATION_FORMAT);
        }
        if (!serdeParams.isEmpty()) {
          appendSerdeParams(tblRowFormat, serdeParams).append(" \n");
        }
        tblRowFormat.append("STORED AS INPUTFORMAT \n  '"
            + HiveStringUtils.escapeHiveCommand(sd.getInputFormat()) + "' \n");
        tblRowFormat.append("OUTPUTFORMAT \n  '" + HiveStringUtils.escapeHiveCommand(sd.getOutputFormat()) + "'");
      } else {
        duplicateProps.add(META_TABLE_STORAGE);
        tblRowFormat.append("STORED BY \n  '" +
            HiveStringUtils.escapeHiveCommand(tbl.getParameters().get(META_TABLE_STORAGE)) + "' \n");
        // SerDe Properties
        if (!serdeParams.isEmpty()) {
          appendSerdeParams(tblRowFormat, serdeInfo.getParameters());
        }
      }
      String tblLocation = "  '" + HiveStringUtils.escapeHiveCommand(sd.getLocation()) + "'";

      // Table properties
      duplicateProps.addAll(StatsSetupConst.TABLE_PARAMS_STATS_KEYS);
      String tblProperties = DDLUtils.propertiesToString(tbl.getParameters(), duplicateProps);

      createTabStmt.add(TEMPORARY, tblTemp);
      createTabStmt.add(EXTERNAL, tblExternal);
      createTabStmt.add(LIST_COLUMNS, tblColumns);
      createTabStmt.add(TBL_COMMENT, tblComment);
      createTabStmt.add(LIST_PARTITIONS, tblPartitions);
      createTabStmt.add(SORT_BUCKET, tblSortBucket);
      createTabStmt.add(SKEWED_INFO, tblSkewedInfo);
      createTabStmt.add(ROW_FORMAT, tblRowFormat);
      // Table location should not be printed with hbase backed tables
      if (needsLocation) {
        createTabStmt.add(TBL_LOCATION, tblLocation);
      }
      createTabStmt.add(TBL_PROPERTIES, tblProperties);

      outStream.write(createTabStmt.render().getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      LOG.info("show create table: ", e);
      return 1;
    }

    return 0;
  }

  public static StringBuilder appendSerdeParams(StringBuilder builder, Map<String, String> serdeParam) {
    serdeParam = new TreeMap<String, String>(serdeParam);
    builder.append("WITH SERDEPROPERTIES ( \n");
    List<String> serdeCols = new ArrayList<String>();
    for (Entry<String, String> entry : serdeParam.entrySet()) {
      serdeCols.add("  '" + entry.getKey() + "'='" + HiveStringUtils.escapeHiveCommand(entry.getValue()) + "'");
    }
    builder.append(StringUtils.join(serdeCols, ", \n")).append(')');
    return builder;
  }
}
