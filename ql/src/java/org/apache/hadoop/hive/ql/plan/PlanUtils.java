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

package org.apache.hadoop.hive.ql.plan;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_LOCATION;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_IS_CTAS;
import static org.apache.hive.common.util.HiveStringUtils.quoteComments;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapOutputFormat;
import org.apache.hadoop.hive.metastore.HiveMetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.ddl.table.create.CreateTableDesc;
import org.apache.hadoop.hive.ql.ddl.view.create.CreateMaterializedViewDesc;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileInputFormat;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.type.ExprNodeTypeCheck;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.util.NullOrdering;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe2;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PlanUtils.
 *
 */
public final class PlanUtils {

  protected static final Logger LOG = LoggerFactory.getLogger("org.apache.hadoop.hive.ql.plan.PlanUtils");

  private static long countForMapJoinDumpFilePrefix = 0;

  /**
   * ExpressionTypes.
   *
   */
  public static enum ExpressionTypes {
    FIELD, JEXL
  };
  public static final String LLAP_OUTPUT_FORMAT_KEY = "Llap";
  private static final String LLAP_OF_SH_CLASS = "org.apache.hadoop.hive.llap.LlapStorageHandler";

  public static synchronized long getCountForMapJoinDumpFilePrefix() {
    return countForMapJoinDumpFilePrefix++;
  }

  @SuppressWarnings("nls")
  public static MapredWork getMapRedWork() {
    return new MapredWork();
  }

  public static TableDesc getDefaultTableDesc(CreateTableDesc directoryDesc,
      String cols, String colTypes ) {
    // TODO: this should have an option for directory to inherit from the parent table,
    //       including bucketing and list bucketing, for the use in compaction when the
    //       latter runs inside a transaction.
    TableDesc ret = getDefaultTableDesc(Integer.toString(Utilities.ctrlaCode), cols,
        colTypes, false);;
    if (directoryDesc == null) {
      return ret;
    }

    try {
      Properties properties = ret.getProperties();

      if (directoryDesc.getFieldDelim() != null) {
        properties.setProperty(
            serdeConstants.FIELD_DELIM, directoryDesc.getFieldDelim());
        properties.setProperty(
            serdeConstants.SERIALIZATION_FORMAT, directoryDesc.getFieldDelim());
      }
      if (directoryDesc.getLineDelim() != null) {
        properties.setProperty(
            serdeConstants.LINE_DELIM, directoryDesc.getLineDelim());
      }
      if (directoryDesc.getCollItemDelim() != null) {
        properties.setProperty(
            serdeConstants.COLLECTION_DELIM, directoryDesc.getCollItemDelim());
      }
      if (directoryDesc.getMapKeyDelim() != null) {
        properties.setProperty(
            serdeConstants.MAPKEY_DELIM, directoryDesc.getMapKeyDelim());
      }
      if (directoryDesc.getFieldEscape() !=null) {
        properties.setProperty(
            serdeConstants.ESCAPE_CHAR, directoryDesc.getFieldEscape());
      }
      if (directoryDesc.getSerName() != null) {
        properties.setProperty(
            serdeConstants.SERIALIZATION_LIB, directoryDesc.getSerName());
      }
      if (directoryDesc.getSerdeProps() != null) {
        properties.putAll(directoryDesc.getSerdeProps());
      }
      if (directoryDesc.getInputFormat() != null){
        ret.setInputFileFormatClass(JavaUtils.loadClass(directoryDesc.getInputFormat()));
      }
      if (directoryDesc.getOutputFormat() != null){
        ret.setOutputFileFormatClass(JavaUtils.loadClass(directoryDesc.getOutputFormat()));
      }
      if (directoryDesc.getNullFormat() != null) {
        properties.setProperty(serdeConstants.SERIALIZATION_NULL_FORMAT,
              directoryDesc.getNullFormat());
      }
      if (directoryDesc.getTblProps() != null) {
        properties.putAll(directoryDesc.getTblProps());
      }

    } catch (ClassNotFoundException e) {
      // mimicking behaviour in CreateTableDesc tableDesc creation
      // returning null table description for output.
      LOG.warn("Unable to find class in getDefaultTableDesc: " + e.getMessage(), e);
      return null;
    }
    return ret;
  }

  /**
   * Generate the table descriptor of MetadataTypedColumnsetSerDe with the
   * separatorCode and column names (comma separated string).
   */
  public static TableDesc getDefaultTableDesc(String separatorCode,
      String columns) {
    return getDefaultTableDesc(separatorCode, columns, false);
  }

  /**
   * Generate the table descriptor of given serde with the separatorCode and
   * column names (comma separated string).
   */
  public static TableDesc getTableDesc(
      Class<? extends Deserializer> serdeClass, String separatorCode,
      String columns) {
    return getTableDesc(serdeClass, separatorCode, columns, false);
  }

  /**
   * Generate the table descriptor of MetadataTypedColumnsetSerDe with the
   * separatorCode and column names (comma separated string), and whether the
   * last column should take the rest of the line.
   */
  public static TableDesc getDefaultTableDesc(String separatorCode,
      String columns, boolean lastColumnTakesRestOfTheLine) {
    return getDefaultTableDesc(separatorCode, columns, null,
        lastColumnTakesRestOfTheLine);
  }

  /**
   * Generate the table descriptor of the serde specified with the separatorCode
   * and column names (comma separated string), and whether the last column
   * should take the rest of the line.
   */
  public static TableDesc getTableDesc(
      Class<? extends Deserializer> serdeClass, String separatorCode,
      String columns, boolean lastColumnTakesRestOfTheLine) {
    return getTableDesc(serdeClass, separatorCode, columns, null, null,
        lastColumnTakesRestOfTheLine);
  }

  /**
   * Generate the table descriptor of MetadataTypedColumnsetSerDe with the
   * separatorCode and column names (comma separated string), and whether the
   * last column should take the rest of the line.
   */
  public static TableDesc getDefaultTableDesc(String separatorCode,
      String columns, String columnTypes, boolean lastColumnTakesRestOfTheLine) {
    return getTableDesc(getDefaultSerDe(), separatorCode, columns,
        columnTypes, null, lastColumnTakesRestOfTheLine);
  }

  public static TableDesc getTableDesc(Class<? extends Deserializer> serdeClass,
      String separatorCode, String columns, String columnTypes, List<FieldSchema> partCols,
      boolean lastColumnTakesRestOfTheLine) {

    return getTableDesc(serdeClass, separatorCode, columns, columnTypes, partCols,
        lastColumnTakesRestOfTheLine, "TextFile");
  }

  public static TableDesc getTableDesc(
      Class<? extends Deserializer> serdeClass, String separatorCode,
      String columns, String columnTypes, List<FieldSchema> partCols, boolean lastColumnTakesRestOfTheLine,
      String fileFormat) {

    Properties properties = Utilities.makeProperties(
        serdeConstants.SERIALIZATION_FORMAT, separatorCode, serdeConstants.LIST_COLUMNS,
        columns);

    if (!separatorCode.equals(Integer.toString(Utilities.ctrlaCode))) {
      properties.setProperty(serdeConstants.FIELD_DELIM, separatorCode);
    }

    if (columnTypes != null) {
      properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, columnTypes);
    }

    if (partCols != null && !partCols.isEmpty()) {
      properties.setProperty(serdeConstants.LIST_PARTITION_COLUMNS,
          MetaStoreUtils.getColumnNamesFromFieldSchema(partCols));
      properties.setProperty(serdeConstants.LIST_PARTITION_COLUMN_TYPES,
          MetaStoreUtils.getColumnTypesFromFieldSchema(partCols, ":"));
      properties.setProperty(serdeConstants.LIST_PARTITION_COLUMN_COMMENTS,
          MetaStoreUtils.getColumnCommentsFromFieldSchema(partCols));
    }

    if (lastColumnTakesRestOfTheLine) {
      properties.setProperty(serdeConstants.SERIALIZATION_LAST_COLUMN_TAKES_REST,
          "true");
    }

    Class inputFormat, outputFormat;
    // get the input & output file formats
    if ("HiveSequenceFile".equalsIgnoreCase(fileFormat)) {
      inputFormat = HiveSequenceFileInputFormat.class;
      outputFormat = SequenceFileOutputFormat.class;
    } else if ("SequenceFile".equalsIgnoreCase(fileFormat)) {
      inputFormat = SequenceFileInputFormat.class;
      outputFormat = SequenceFileOutputFormat.class;
    } else if ("RCFile".equalsIgnoreCase(fileFormat)) {
      inputFormat = RCFileInputFormat.class;
      outputFormat = RCFileOutputFormat.class;
      assert serdeClass == ColumnarSerDe.class;
    } else if (LLAP_OUTPUT_FORMAT_KEY.equalsIgnoreCase(fileFormat)) {
      inputFormat = TextInputFormat.class;
      outputFormat = LlapOutputFormat.class;
      properties.setProperty(hive_metastoreConstants.META_TABLE_STORAGE, LLAP_OF_SH_CLASS);
    } else { // use TextFile by default
      inputFormat = TextInputFormat.class;
      outputFormat = IgnoreKeyTextOutputFormat.class;
    }
    properties.setProperty(serdeConstants.SERIALIZATION_LIB, serdeClass.getName());
    properties.setProperty(hive_metastoreConstants.TABLE_BUCKETING_VERSION, "-1");
    return new TableDesc(inputFormat, outputFormat, properties);
  }

  public static TableDesc getDefaultQueryOutputTableDesc(String cols, String colTypes,
      String fileFormat, Class<? extends Deserializer> serdeClass) {
    TableDesc tblDesc =
        getTableDesc(serdeClass, "" + Utilities.ctrlaCode, cols, colTypes, null, false, fileFormat);
    // enable escaping
    tblDesc.getProperties().setProperty(serdeConstants.ESCAPE_CHAR, "\\");
    tblDesc.getProperties().setProperty(serdeConstants.SERIALIZATION_ESCAPE_CRLF, "true");
    // enable extended nesting levels
    tblDesc.getProperties().setProperty(
        LazySerDeParameters.SERIALIZATION_EXTEND_ADDITIONAL_NESTING_LEVELS, "true");
    return tblDesc;
  }

 /**
   * Generate a table descriptor from a createTableDesc.
   */
  public static TableDesc getTableDesc(CreateTableDesc crtTblDesc, String cols,
      String colTypes) {

    TableDesc ret;

    // Resolve storage handler (if any)
    try {
      HiveStorageHandler storageHandler = null;
      if (crtTblDesc.getStorageHandler() != null) {
        storageHandler = HiveUtils.getStorageHandler(
                SessionState.getSessionConf(), crtTblDesc.getStorageHandler());
      }

      Class<? extends Deserializer> serdeClass = getDefaultSerDe();
      String separatorCode = Integer.toString(Utilities.ctrlaCode);
      String columns = cols;
      String columnTypes = colTypes;
      boolean lastColumnTakesRestOfTheLine = false;

      if (storageHandler != null) {
        serdeClass = storageHandler.getSerDeClass();
      } else if (crtTblDesc.getSerName() != null) {
        serdeClass = JavaUtils.loadClass(crtTblDesc.getSerName());
      }

      if (crtTblDesc.getFieldDelim() != null) {
        separatorCode = crtTblDesc.getFieldDelim();
      }

      ret = getTableDesc(serdeClass, separatorCode, columns, columnTypes, crtTblDesc.getPartCols(),
          lastColumnTakesRestOfTheLine);

      // set other table properties
      Properties properties = ret.getProperties();

      if (crtTblDesc.getStorageHandler() != null) {
        properties.setProperty(
                org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE,
                crtTblDesc.getStorageHandler());
        if (isNotBlank(crtTblDesc.getLocation())) {
          properties.setProperty(META_TABLE_LOCATION, crtTblDesc.getLocation());
        }
      }

      if (crtTblDesc.getCollItemDelim() != null) {
        properties.setProperty(serdeConstants.COLLECTION_DELIM, crtTblDesc
            .getCollItemDelim());
      }

      if (crtTblDesc.getMapKeyDelim() != null) {
        properties.setProperty(serdeConstants.MAPKEY_DELIM, crtTblDesc
            .getMapKeyDelim());
      }

      if (crtTblDesc.getFieldEscape() != null) {
        properties.setProperty(serdeConstants.ESCAPE_CHAR, crtTblDesc
            .getFieldEscape());
      }

      if (crtTblDesc.getLineDelim() != null) {
        properties.setProperty(serdeConstants.LINE_DELIM, crtTblDesc.getLineDelim());
      }

      if (crtTblDesc.getNullFormat() != null) {
        properties.setProperty(serdeConstants.SERIALIZATION_NULL_FORMAT,
              crtTblDesc.getNullFormat());
      }

      if (crtTblDesc.getDbTableName() != null && crtTblDesc.getDatabaseName() != null) {
        properties.setProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_NAME,
            crtTblDesc.getDbTableName());
      }

      if (crtTblDesc.getTblProps() != null) {
        properties.putAll(crtTblDesc.getTblProps());
      }
      if (crtTblDesc.getSerdeProps() != null) {
        properties.putAll(crtTblDesc.getSerdeProps());
      }

      // replace the default input & output file format with those found in
      // crtTblDesc
      Class<? extends InputFormat> in_class;
      if (storageHandler != null) {
        in_class = storageHandler.getInputFormatClass();
      } else {
        in_class = JavaUtils.loadClass(crtTblDesc.getInputFormat());
      }
      Class<? extends OutputFormat> out_class;
      if (storageHandler != null) {
        out_class = storageHandler.getOutputFormatClass();
      } else {
        out_class = JavaUtils.loadClass(crtTblDesc.getOutputFormat());
      }
      ret.setInputFileFormatClass(in_class);
      ret.setOutputFileFormatClass(out_class);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Unable to find class in getTableDesc: " + e.getMessage(), e);
    } catch (HiveException e) {
      throw new RuntimeException("Error loading storage handler in getTableDesc: " + e.getMessage(), e);
    }
    return ret;
  }

 /**
   * Generate a table descriptor from a createViewDesc.
   */
  public static TableDesc getTableDesc(CreateMaterializedViewDesc crtViewDesc, String cols, String colTypes) {
    TableDesc ret;

    try {
      HiveStorageHandler storageHandler = null;
      if (crtViewDesc.getStorageHandler() != null) {
        storageHandler = HiveUtils.getStorageHandler(
                SessionState.getSessionConf(), crtViewDesc.getStorageHandler());
      }

      Class<? extends Deserializer> serdeClass = getDefaultSerDe();
      String separatorCode = Integer.toString(Utilities.ctrlaCode);
      String columns = cols;
      String columnTypes = colTypes;
      boolean lastColumnTakesRestOfTheLine = false;

      if (storageHandler != null) {
        serdeClass = storageHandler.getSerDeClass();
      } else if (crtViewDesc.getSerde() != null) {
        serdeClass = JavaUtils.loadClass(crtViewDesc.getSerde());
      }

      ret = getTableDesc(serdeClass, separatorCode, columns, columnTypes, crtViewDesc.getPartCols(),
          lastColumnTakesRestOfTheLine);

      // set other table properties
      Properties properties = ret.getProperties();

      if (crtViewDesc.getStorageHandler() != null) {
        properties.setProperty(
                org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE,
                crtViewDesc.getStorageHandler());
        if (isNotBlank(crtViewDesc.getLocation())) {
          ret.getProperties().setProperty(META_TABLE_LOCATION, crtViewDesc.getLocation());
        }
      }

      if (crtViewDesc.getViewName() != null) {
        properties.setProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_NAME,
            crtViewDesc.getViewName());
      }

      if (crtViewDesc.getTblProps() != null) {
        properties.putAll(crtViewDesc.getTblProps());
      }
      if (crtViewDesc.getSerdeProps() != null) {
        properties.putAll(crtViewDesc.getSerdeProps());
      }

      // replace the default input & output file format with those found in
      // crtTblDesc
      Class<? extends InputFormat> in_class;
      if (storageHandler != null) {
        in_class = storageHandler.getInputFormatClass();
      } else {
        in_class = JavaUtils.loadClass(crtViewDesc.getInputFormat());
      }
      Class<? extends OutputFormat> out_class;
      if (storageHandler != null) {
        out_class = storageHandler.getOutputFormatClass();
      } else {
        out_class = JavaUtils.loadClass(crtViewDesc.getOutputFormat());
      }
      ret.setInputFileFormatClass(in_class);
      ret.setOutputFileFormatClass(out_class);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Unable to find class in getTableDesc: " + e.getMessage(), e);
    } catch (HiveException e) {
      throw new RuntimeException("Error loading storage handler in getTableDesc: " + e.getMessage(), e);
    }
    return ret;
  }

  /**
   * Generate the table descriptor for reduce key.
   */
  public static TableDesc getReduceKeyTableDesc(List<FieldSchema> fieldSchemas,
      String order, String nullOrder) {
    return new TableDesc(
        SequenceFileInputFormat.class, SequenceFileOutputFormat.class,
        Utilities.makeProperties(serdeConstants.LIST_COLUMNS, MetaStoreUtils
        .getColumnNamesFromFieldSchema(fieldSchemas),
        serdeConstants.COLUMN_NAME_DELIMITER, MetaStoreUtils.getColumnNameDelimiter(fieldSchemas),
        serdeConstants.LIST_COLUMN_TYPES, MetaStoreUtils
        .getColumnTypesFromFieldSchema(fieldSchemas),
        serdeConstants.SERIALIZATION_SORT_ORDER, order,
        serdeConstants.SERIALIZATION_NULL_SORT_ORDER, nullOrder,
        serdeConstants.SERIALIZATION_LIB, BinarySortableSerDe.class.getName()));
  }

  /**
   * Generate the table descriptor for Map-side join key.
   */
  public static TableDesc getMapJoinKeyTableDesc(Configuration conf,
      List<FieldSchema> fieldSchemas) {
    if (HiveConf.getVar(conf, ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
      // In tez we use a different way of transmitting the hash table.
      // We basically use ReduceSinkOperators and set the transfer to
      // be broadcast (instead of partitioned). As a consequence we use
      // a different SerDe than in the MR mapjoin case.
      StringBuilder order = new StringBuilder();
      StringBuilder nullOrder = new StringBuilder();
      for (FieldSchema f: fieldSchemas) {
        order.append("+");
        nullOrder.append(NullOrdering.defaultNullOrder(conf).getSign());
      }
      return new TableDesc(
          SequenceFileInputFormat.class, SequenceFileOutputFormat.class,
          Utilities.makeProperties(serdeConstants.LIST_COLUMNS, MetaStoreUtils
              .getColumnNamesFromFieldSchema(fieldSchemas),
              serdeConstants.COLUMN_NAME_DELIMITER, MetaStoreUtils.getColumnNameDelimiter(fieldSchemas),
              serdeConstants.LIST_COLUMN_TYPES, MetaStoreUtils
              .getColumnTypesFromFieldSchema(fieldSchemas),
              serdeConstants.SERIALIZATION_SORT_ORDER, order.toString(),
              serdeConstants.SERIALIZATION_NULL_SORT_ORDER, nullOrder.toString(),
              serdeConstants.SERIALIZATION_LIB, BinarySortableSerDe.class.getName()));
    } else {
      return new TableDesc(SequenceFileInputFormat.class,
          SequenceFileOutputFormat.class, Utilities.makeProperties("columns",
              MetaStoreUtils.getColumnNamesFromFieldSchema(fieldSchemas),
              "columns.types", MetaStoreUtils
              .getColumnTypesFromFieldSchema(fieldSchemas),
              serdeConstants.ESCAPE_CHAR, "\\",
              serdeConstants.SERIALIZATION_LIB,LazyBinarySerDe.class.getName()));
    }
  }

  /**
   * Generate the table descriptor for Map-side join value.
   */
  public static TableDesc getMapJoinValueTableDesc(
      List<FieldSchema> fieldSchemas) {
      return new TableDesc(SequenceFileInputFormat.class,
          SequenceFileOutputFormat.class, Utilities.makeProperties(
              serdeConstants.LIST_COLUMNS, MetaStoreUtils
              .getColumnNamesFromFieldSchema(fieldSchemas),
              serdeConstants.COLUMN_NAME_DELIMITER, MetaStoreUtils.getColumnNameDelimiter(fieldSchemas),
              serdeConstants.LIST_COLUMN_TYPES, MetaStoreUtils
              .getColumnTypesFromFieldSchema(fieldSchemas),
              serdeConstants.ESCAPE_CHAR, "\\",
              serdeConstants.SERIALIZATION_LIB,LazyBinarySerDe.class.getName()));
  }

  /**
   * Generate the table descriptor for intermediate files.
   */
  public static TableDesc getIntermediateFileTableDesc(
      List<FieldSchema> fieldSchemas) {
    return new TableDesc(SequenceFileInputFormat.class,
        SequenceFileOutputFormat.class, Utilities.makeProperties(
        serdeConstants.LIST_COLUMNS, MetaStoreUtils
        .getColumnNamesFromFieldSchema(fieldSchemas),
        serdeConstants.COLUMN_NAME_DELIMITER, MetaStoreUtils.getColumnNameDelimiter(fieldSchemas),
        serdeConstants.COLUMN_NAME_DELIMITER, MetaStoreUtils.getColumnNameDelimiter(fieldSchemas),
        serdeConstants.LIST_COLUMN_TYPES, MetaStoreUtils
        .getColumnTypesFromFieldSchema(fieldSchemas),
        serdeConstants.ESCAPE_CHAR, "\\",
        serdeConstants.SERIALIZATION_LIB,LazyBinarySerDe.class.getName()));
  }

  /**
   * Generate the table descriptor for intermediate files.
   */
  public static TableDesc getReduceValueTableDesc(List<FieldSchema> fieldSchemas) {
    return new TableDesc(SequenceFileInputFormat.class,
        SequenceFileOutputFormat.class, Utilities.makeProperties(
        serdeConstants.LIST_COLUMNS, MetaStoreUtils
        .getColumnNamesFromFieldSchema(fieldSchemas),
        serdeConstants.LIST_COLUMN_TYPES, MetaStoreUtils
        .getColumnTypesFromFieldSchema(fieldSchemas),
        serdeConstants.ESCAPE_CHAR, "\\",
        serdeConstants.SERIALIZATION_LIB, LazyBinarySerDe2.class.getName()));
  }

  /**
   * Convert the ColumnList to FieldSchema list.
   *
   * Adds union type for distinctColIndices.
   */
  public static List<FieldSchema> getFieldSchemasFromColumnListWithLength(
      List<ExprNodeDesc> cols, List<List<Integer>> distinctColIndices,
      List<String> outputColumnNames, int length,
      String fieldPrefix) {
    // last one for union column.
    List<FieldSchema> schemas = new ArrayList<FieldSchema>(length + 1);
    for (int i = 0; i < length; i++) {
      schemas.add(HiveMetaStoreUtils.getFieldSchemaFromTypeInfo(
          fieldPrefix + outputColumnNames.get(i), cols.get(i).getTypeInfo()));
    }

    List<TypeInfo> unionTypes = new ArrayList<TypeInfo>();
    for (List<Integer> distinctCols : distinctColIndices) {
      List<String> names = new ArrayList<String>();
      List<TypeInfo> types = new ArrayList<TypeInfo>();
      int numExprs = 0;
      for (int i : distinctCols) {
        names.add(HiveConf.getColumnInternalName(numExprs));
        types.add(cols.get(i).getTypeInfo());
        numExprs++;
      }
      unionTypes.add(TypeInfoFactory.getStructTypeInfo(names, types));
    }
    if (outputColumnNames.size() - length > 0) {
      schemas.add(HiveMetaStoreUtils.getFieldSchemaFromTypeInfo(
          fieldPrefix + outputColumnNames.get(length),
          TypeInfoFactory.getUnionTypeInfo(unionTypes)));
    }

    return schemas;
  }

  /**
   * Convert the ColumnList to FieldSchema list.
   */
  public static List<FieldSchema> getFieldSchemasFromColumnList(
      List<ExprNodeDesc> cols, List<String> outputColumnNames, int start,
      String fieldPrefix) {
    List<FieldSchema> schemas = new ArrayList<FieldSchema>(cols.size());
    for (int i = 0; i < cols.size(); i++) {
      schemas.add(HiveMetaStoreUtils.getFieldSchemaFromTypeInfo(fieldPrefix
          + outputColumnNames.get(i + start), cols.get(i).getTypeInfo()));
    }
    return schemas;
  }

  /**
   * Convert the ColumnList to FieldSchema list.
   */
  public static List<FieldSchema> getFieldSchemasFromColumnList(
      List<ExprNodeDesc> cols, String fieldPrefix) {
    List<FieldSchema> schemas = new ArrayList<FieldSchema>(cols.size());
    for (int i = 0; i < cols.size(); i++) {
      schemas.add(HiveMetaStoreUtils.getFieldSchemaFromTypeInfo(fieldPrefix + i,
          cols.get(i).getTypeInfo()));
    }
    return schemas;
  }

  /**
   * Convert the RowSchema to FieldSchema list.
   */
  public static List<FieldSchema> getFieldSchemasFromRowSchema(RowSchema row,
      String fieldPrefix) {
    List<ColumnInfo> c = row.getSignature();
    return getFieldSchemasFromColumnInfo(c, fieldPrefix);
  }

  /**
   * Convert the ColumnInfo to FieldSchema.
   */
  public static List<FieldSchema> getFieldSchemasFromColumnInfo(
      List<ColumnInfo> cols, String fieldPrefix) {
    if ((cols == null) || (cols.size() == 0)) {
      return Collections.emptyList();
    }

    List<FieldSchema> schemas = new ArrayList<FieldSchema>(cols.size());
    for (int i = 0; i < cols.size(); i++) {
      String name = cols.get(i).getInternalName();
      if (name.equals(String.valueOf(i))) {
        name = fieldPrefix + name;
      }
      schemas.add(HiveMetaStoreUtils.getFieldSchemaFromTypeInfo(name, cols.get(i)
          .getType()));
    }
    return schemas;
  }

  public static List<FieldSchema> sortFieldSchemas(List<FieldSchema> schema) {
    Collections.sort(schema, new Comparator<FieldSchema>() {

      @Override
      public int compare(FieldSchema o1, FieldSchema o2) {
        return o1.getName().compareTo(o2.getName());
      }

    });
    return schema;
  }

  /**
   * Create the reduce sink descriptor.
   *
   * @param keyCols
   *          The columns to be stored in the key
   * @param valueCols
   *          The columns to be stored in the value
   * @param outputColumnNames
   *          The output columns names
   * @param tag
   *          The tag for this reducesink
   * @param partitionCols
   *          The columns for partitioning.
   * @param numReducers
   *          The number of reducers, set to -1 for automatic inference based on
   *          input data size.
   * @param writeType Whether this is an Acid write, and if so whether it is insert, update,
   *                  or delete.
   * @return The reduceSinkDesc object.
   */
  public static ReduceSinkDesc getReduceSinkDesc(
      List<ExprNodeDesc> keyCols, List<ExprNodeDesc> valueCols,
      List<String> outputColumnNames, boolean includeKeyCols, int tag,
      List<ExprNodeDesc> partitionCols, String order, String nullOrder, NullOrdering defaultNullOrder,
      int numReducers, AcidUtils.Operation writeType, boolean isCompaction) {
    ReduceSinkDesc reduceSinkDesc = getReduceSinkDesc(keyCols, keyCols.size(), valueCols,
            new ArrayList<List<Integer>>(),
            includeKeyCols ? outputColumnNames.subList(0, keyCols.size()) :
                    new ArrayList<String>(),
            includeKeyCols ? outputColumnNames.subList(keyCols.size(),
                    outputColumnNames.size()) : outputColumnNames,
            includeKeyCols, tag, partitionCols, order, nullOrder, defaultNullOrder, numReducers, writeType);
    reduceSinkDesc.setIsCompaction(isCompaction);
    return reduceSinkDesc;
  }

  /**
   * Create the reduce sink descriptor.
   *
   * @param keyCols
   *          The columns to be stored in the key
   * @param numKeys
   *          number of distribution key numbers. Equals to group-by-key
   *          numbers usually.
   * @param valueCols
   *          The columns to be stored in the value
   * @param distinctColIndices
   *          column indices for distinct aggregate parameters
   * @param outputKeyColumnNames
   *          The output key columns names
   * @param outputValueColumnNames
   *          The output value columns names
   * @param tag
   *          The tag for this reducesink
   * @param partitionCols
   *          The columns for partitioning.
   * @param numReducers
   *          The number of reducers, set to -1 for automatic inference based on
   *          input data size.
   * @param writeType Whether this is an Acid write, and if so whether it is insert, update,
   *                  or delete.
   * @return The reduceSinkDesc object.
   */
  public static ReduceSinkDesc getReduceSinkDesc(
      final List<ExprNodeDesc> keyCols, int numKeys,
      List<ExprNodeDesc> valueCols,
      List<List<Integer>> distinctColIndices,
      List<String> outputKeyColumnNames,
      List<String> outputValueColumnNames,
      boolean includeKeyCols, int tag,
      List<ExprNodeDesc> partitionCols, String order, String nullOrder, NullOrdering defaultNullOrder,
      int numReducers, AcidUtils.Operation writeType) {
    TableDesc keyTable = null;
    TableDesc valueTable = null;
    List<String> outputKeyCols = new ArrayList<String>();
    List<String> outputValCols = new ArrayList<String>();
    if (includeKeyCols) {
      List<FieldSchema> keySchema = getFieldSchemasFromColumnListWithLength(
          keyCols, distinctColIndices, outputKeyColumnNames, numKeys, "");
      if (order.length() < outputKeyColumnNames.size()) {
        order = order + "+";
      }
      if (nullOrder.length() < outputKeyColumnNames.size()) {
        nullOrder = nullOrder + defaultNullOrder.getSign();
      }
      keyTable = getReduceKeyTableDesc(keySchema, order, nullOrder);
      outputKeyCols.addAll(outputKeyColumnNames);
    } else {
      keyTable = getReduceKeyTableDesc(getFieldSchemasFromColumnList(
          keyCols, "reducesinkkey"), order, nullOrder);
     for (int i = 0; i < keyCols.size(); i++) {
        outputKeyCols.add("reducesinkkey" + i);
      }
    }
    valueTable = getReduceValueTableDesc(getFieldSchemasFromColumnList(
        valueCols, outputValueColumnNames, 0, ""));
    outputValCols.addAll(outputValueColumnNames);
    return new ReduceSinkDesc(keyCols, numKeys, valueCols, outputKeyCols,
        distinctColIndices, outputValCols,
        tag, partitionCols, numReducers, keyTable,
        valueTable, writeType);
  }

  /**
   * Create the reduce sink descriptor.
   *
   * @param keyCols
   *          The columns to be stored in the key
   * @param valueCols
   *          The columns to be stored in the value
   * @param outputColumnNames
   *          The output columns names
   * @param tag
   *          The tag for this reducesink
   * @param numPartitionFields
   *          The first numPartitionFields of keyCols will be partition columns.
   *          If numPartitionFields=-1, then partition randomly.
   * @param numReducers
   *          The number of reducers, set to -1 for automatic inference based on
   *          input data size.
   * @param writeType Whether this is an Acid write, and if so whether it is insert, update,
   *                  or delete.
   * @return The reduceSinkDesc object.
   */
  public static ReduceSinkDesc getReduceSinkDesc(
      List<ExprNodeDesc> keyCols, List<ExprNodeDesc> valueCols,
      List<String> outputColumnNames, boolean includeKey, int tag,
      int numPartitionFields, int numReducers, AcidUtils.Operation writeType,
      NullOrdering defaultNullOrder)
      throws SemanticException {
    return getReduceSinkDesc(keyCols, keyCols.size(), valueCols,
        new ArrayList<List<Integer>>(),
        includeKey ? outputColumnNames.subList(0, keyCols.size()) :
          new ArrayList<String>(),
        includeKey ?
            outputColumnNames.subList(keyCols.size(), outputColumnNames.size())
            : outputColumnNames,
        includeKey, tag, numPartitionFields, numReducers, writeType, defaultNullOrder);
  }

  /**
   * Create the reduce sink descriptor.
   *
   * @param keyCols
   *          The columns to be stored in the key
   * @param numKeys  number of distribution keys. Equals to group-by-key
   *        numbers usually.
   * @param valueCols
   *          The columns to be stored in the value
   * @param distinctColIndices
   *          column indices for distinct aggregates
   * @param outputKeyColumnNames
   *          The output key columns names
   * @param outputValueColumnNames
   *          The output value columns names
   * @param tag
   *          The tag for this reducesink
   * @param numPartitionFields
   *          The first numPartitionFields of keyCols will be partition columns.
   *          If numPartitionFields=-1, then partition randomly.
   * @param numReducers
   *          The number of reducers, set to -1 for automatic inference based on
   *          input data size.
   * @param writeType Whether this is an Acid write, and if so whether it is insert, update,
   *                  or delete.
   * @return The reduceSinkDesc object.
   */
  public static ReduceSinkDesc getReduceSinkDesc(
      List<ExprNodeDesc> keyCols, int numKeys, List<ExprNodeDesc> valueCols,
      List<List<Integer>> distinctColIndices,
      List<String> outputKeyColumnNames, List<String> outputValueColumnNames,
      boolean includeKey, int tag,
      int numPartitionFields, int numReducers, AcidUtils.Operation writeType,
      NullOrdering defaultNullOrder)
      throws SemanticException {

    ArrayList<ExprNodeDesc> partitionCols = new ArrayList<ExprNodeDesc>();
    if (numPartitionFields >= keyCols.size()) {
      partitionCols.addAll(keyCols);
    } else if (numPartitionFields >= 0) {
      partitionCols.addAll(keyCols.subList(0, numPartitionFields));
    } else {
      // numPartitionFields = -1 means random partitioning
      partitionCols.add(ExprNodeTypeCheck.getExprNodeDefaultExprProcessor().
          getFuncExprNodeDesc("rand"));
    }

    StringBuilder order = new StringBuilder();
    StringBuilder nullOrder = new StringBuilder();
    for (int i = 0; i < keyCols.size(); i++) {
      order.append("+");
      nullOrder.append(defaultNullOrder.getSign());
    }
    return getReduceSinkDesc(keyCols, numKeys, valueCols, distinctColIndices,
        outputKeyColumnNames, outputValueColumnNames, includeKey, tag,
        partitionCols, order.toString(), nullOrder.toString(), defaultNullOrder, numReducers, writeType);
  }

  /**
   * Loads the storage handler (if one exists) for the given table
   * and invokes {@link HiveStorageHandler#configureInputJobProperties(TableDesc, java.util.Map)}.
   *
   * @param tableDesc table descriptor
   */
  public static void configureInputJobPropertiesForStorageHandler(TableDesc tableDesc) {
      configureJobPropertiesForStorageHandler(true,tableDesc);
  }

  /**
   * Loads the storage handler (if one exists) for the given table
   * and invokes {@link HiveStorageHandler#configureOutputJobProperties(TableDesc, java.util.Map)}.
   *
   * @param tableDesc table descriptor
   */
  public static void configureOutputJobPropertiesForStorageHandler(TableDesc tableDesc) {
      configureJobPropertiesForStorageHandler(false,tableDesc);
  }

  private static void configureJobPropertiesForStorageHandler(boolean input,
    TableDesc tableDesc) {

    if (tableDesc == null) {
      return;
    }

    try {
      HiveStorageHandler storageHandler =
        HiveUtils.getStorageHandler(
          Hive.get().getConf(),
          tableDesc.getProperties().getProperty(
            org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE));
      if (storageHandler != null) {
        Map<String, String> jobProperties = new LinkedHashMap<String, String>();
        Map<String, String> jobSecrets = new LinkedHashMap<String, String>();
        if(input) {
            try {
                storageHandler.configureInputJobProperties(
                  tableDesc,
                  jobProperties);
            } catch(AbstractMethodError e) {
                LOG.info("configureInputJobProperties not found "+
                    "using configureTableJobProperties",e);
                storageHandler.configureTableJobProperties(tableDesc, jobProperties);
            }

            try{
              storageHandler.configureInputJobCredentials(
                tableDesc,
                jobSecrets);
            } catch(AbstractMethodError e) {
              // ignore
              LOG.info("configureInputJobSecrets not found");
            }
        }
        else {
            try {
                storageHandler.configureOutputJobProperties(
                  tableDesc,
                  jobProperties);
            } catch(AbstractMethodError e) {
                LOG.info("configureOutputJobProperties not found"+
                    "using configureTableJobProperties",e);
                storageHandler.configureTableJobProperties(tableDesc, jobProperties);
            }
        }
        // Job properties are only relevant for non-native tables, so
        // for native tables, leave it null to avoid cluttering up
        // plans.
        if (!jobProperties.isEmpty()) {
          tableDesc.setJobProperties(jobProperties);
        }

        // same idea, only set for non-native tables
        if (!jobSecrets.isEmpty()) {
          tableDesc.setJobSecrets(jobSecrets);
        }
      }
    } catch (HiveException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    String handlerClass = tableDesc.getProperties().getProperty(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE);
    try {
      HiveStorageHandler storageHandler = HiveUtils.getStorageHandler(jobConf, handlerClass);
      if (storageHandler != null) {
        storageHandler.configureJobConf(tableDesc, jobConf);
      }
      if (tableDesc.getJobSecrets() != null) {
        for (Map.Entry<String, String> entry : tableDesc.getJobSecrets().entrySet()) {
          String key = TableDesc.SECRET_PREFIX + TableDesc.SECRET_DELIMIT +
                  tableDesc.getTableName() + TableDesc.SECRET_DELIMIT + entry.getKey();
          jobConf.getCredentials().addSecretKey(new Text(key), entry.getValue().getBytes());
        }
        tableDesc.getJobSecrets().clear();
      }
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

  public static String stripQuotes(String val) {
    if ((val.charAt(0) == '\'' && val.charAt(val.length() - 1) == '\'')
        || (val.charAt(0) == '\"' && val.charAt(val.length() - 1) == '\"')) {
      val = val.substring(1, val.length() - 1);
    }
    return val;
  }

  /**
   * Remove prefix from "Path -&gt; Alias"
   * This is required for testing.
   * In order to verify that path is right, we need to display it in expected test result.
   * But, mask pattern masks path with some patterns.
   * So, we need to remove prefix from path which triggers mask pattern.
   * @param origiKey
   * @return
   */
  public static String removePrefixFromWarehouseConfig(String origiKey) {
    String prefix = SessionState.get().getConf().getVar(HiveConf.ConfVars.METASTORE_WAREHOUSE);
    if ((prefix != null) && (prefix.length() > 0)) {
      //Local file system is using pfile:/// {@link ProxyLocalFileSystem}
      prefix = prefix.replace("pfile:///", "pfile:/");
      int index = origiKey.indexOf(prefix);
      if (index > -1) {
        origiKey = origiKey.substring(index + prefix.length());
      }
    }
    return origiKey;
  }

  private PlanUtils() {
    // prevent instantiation
  }

  public static ReadEntity addInput(Set<ReadEntity> inputs, ReadEntity newInput) {
    return addInput(inputs,newInput,false);
  }

  // Add the input 'newInput' to the set of inputs for the query.
  // The input may or may not be already present.
  // The ReadEntity also contains the parents from it is derived (only populated
  // in case of views). The equals method for ReadEntity does not compare the parents
  // so that the same input with different parents cannot be added twice. If the input
  // is already present, make sure the parents are added.
  // Consider the query:
  // select * from (select * from V2 union all select * from V3) subq;
  // where both V2 and V3 depend on V1 (eg V2 : select * from V1, V3: select * from V1),
  // addInput would be called twice for V1 (one with parent V2 and the other with parent V3).
  // When addInput is called for the first time for V1, V1 (parent V2) is added to inputs.
  // When addInput is called for the second time for V1, the input V1 from inputs is picked up,
  // and it's parents are enhanced to include V2 and V3
  // The inputs will contain: (V2, no parent), (V3, no parent), (V1, parents(V2, v3))
  //
  // If the ReadEntity is already present and another ReadEntity with same name is
  // added, then the isDirect flag is updated to be the OR of values of both.
  // mergeIsDirectFlag, need to merge isDirect flag even newInput does not have parent
  public static ReadEntity addInput(Set<ReadEntity> inputs, ReadEntity newInput, boolean mergeIsDirectFlag) {
    // If the input is already present, make sure the new parent is added to the input.
    if (inputs.contains(newInput)) {
      for (ReadEntity input : inputs) {
        if (input.equals(newInput)) {
          if ((newInput.getParents() != null) && (!newInput.getParents().isEmpty())) {
            input.getParents().addAll(newInput.getParents());
            input.setDirect(input.isDirect() || newInput.isDirect());
          } else if (mergeIsDirectFlag) {
            input.setDirect(input.isDirect() || newInput.isDirect());
          }
          return input;
        }
      }
      assert false;
    } else {
      inputs.add(newInput);
      return newInput;
    }
    // make compile happy
    return null;
  }

  public static String getExprListString(Collection<? extends ExprNodeDesc> exprs) {
    return getExprListString(exprs, false, false);
  }

  public static String getExprListString(Collection<?  extends ExprNodeDesc> exprs, boolean userLevelExplain) {
    return getExprListString(exprs, userLevelExplain, false);
  }

  public static String getExprListString(Collection<?  extends ExprNodeDesc> exprs,
          boolean userLevelExplain, boolean sortExpressions) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (ExprNodeDesc expr: exprs) {
      if (!first) {
        sb.append(", ");
      } else {
        first = false;
      }
      addExprToStringBuffer(expr, sb, userLevelExplain, sortExpressions);
    }
    return sb.length() == 0 ? null : sb.toString();
  }

  public static void addExprToStringBuffer(ExprNodeDesc expr, Appendable sb, boolean userLevelExplain) {
    addExprToStringBuffer(expr, sb, userLevelExplain, false);
  }

  public static void addExprToStringBuffer(ExprNodeDesc expr, Appendable sb,
          boolean userLevelExplain, boolean sortExpressions) {
    try {
      sb.append(expr.getExprString(sortExpressions));
      if (!userLevelExplain) {
        sb.append(" (type: ");
        sb.append(expr.getTypeString());
        sb.append(")");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Check if the table is the temporary table created by VALUES() syntax
   * @param tableName table name
   * @return
   */
  public static boolean isValuesTempTable(String tableName) {
    return tableName.toLowerCase().startsWith(SemanticAnalyzer.VALUES_TMP_TABLE_NAME_PREFIX.toLowerCase());
  }

  public static void addPartitionInputs(Collection<Partition> parts, Collection<ReadEntity> inputs,
      ReadEntity parentViewInfo, boolean isDirectRead) {
    // Store the inputs in a HashMap since we can't get a ReadEntity from inputs since it is
    // implemented as a set.ReadEntity is used as the key so that the HashMap has the same behavior
    // of equals and hashCode
    Map<ReadEntity, ReadEntity> readEntityMap =
        new LinkedHashMap<ReadEntity, ReadEntity>(inputs.size());
    for (ReadEntity input : inputs) {
      readEntityMap.put(input, input);
    }

    for (Partition part : parts) {
      // Don't add the partition or table created during the execution as the input source
      if (isValuesTempTable(part.getTable().getTableName())) {
        continue;
      }

      ReadEntity newInput = null;
      if (part.getTable().isPartitioned()) {
        newInput = new ReadEntity(part, parentViewInfo, isDirectRead);
      } else {
        newInput = new ReadEntity(part.getTable(), parentViewInfo, isDirectRead);
      }

      if (readEntityMap.containsKey(newInput)) {
        ReadEntity input = readEntityMap.get(newInput);
        if ((newInput.getParents() != null) && (!newInput.getParents().isEmpty())) {
          input.getParents().addAll(newInput.getParents());
          input.setDirect(input.isDirect() || newInput.isDirect());
        }
      } else {
        readEntityMap.put(newInput, newInput);
      }
    }

    // Add the new ReadEntity that were added to readEntityMap in PlanUtils.addInput
    if (inputs.size() != readEntityMap.size()) {
      inputs.addAll(readEntityMap.keySet());
    }
  }

  public static void addInputsForView(ParseContext parseCtx) throws HiveException {
    Set<ReadEntity> inputs = parseCtx.getSemanticInputs();
    for (Map.Entry<String, TableScanOperator> entry : parseCtx.getTopOps().entrySet()) {
      String alias = entry.getKey();
      TableScanOperator topOp = entry.getValue();
      ReadEntity parentViewInfo = getParentViewInfo(alias, parseCtx.getViewAliasToInput());

      // Adds tables only for create view (PPD filter can be appended by outer query)
      Table table = topOp.getConf().getTableMetadata();
      PlanUtils.addInput(inputs, new ReadEntity(table, parentViewInfo, parentViewInfo == null));
    }
  }

  public static ReadEntity getParentViewInfo(String alias_id,
      Map<String, ReadEntity> viewAliasToInput) {
    String[] aliases = alias_id.split(":");

    String currentAlias = null;
    ReadEntity currentInput = null;
    // Find the immediate parent possible.
    // For eg: for a query like 'select * from V3', where V3 -> V2, V2 -> V1, V1 -> T
    // -> implies depends on.
    // T's parent would be V1
    // do not check last alias in the array for parent can not be itself.
    for (int pos = 0; pos < aliases.length -1; pos++) {
      currentAlias = currentAlias == null ? aliases[pos] : currentAlias + ":" + aliases[pos];

      currentAlias = currentAlias.replace(SemanticAnalyzer.SUBQUERY_TAG_1, "")
          .replace(SemanticAnalyzer.SUBQUERY_TAG_2, "");
      ReadEntity input = viewAliasToInput.get(currentAlias);
      if (input == null && currentInput != null) {
        // To handle the case of - select * from (select * from V1) A;
        // the currentInput != null check above is needed.
        // the alias list that case would be A:V1:T. Lookup on A would return null,
        // we need to go further to find the view inside it.
        return currentInput;
      }
      currentInput = input;
    }

    return currentInput;
  }

  /**
   * Returns the default SerDe for table and materialized view creation
   * if none is specified.
   */
  public static Class<? extends AbstractSerDe> getDefaultSerDe() {
    return LazySimpleSerDe.class;
  }

  private static final String[] FILTER_OUT_FROM_EXPLAIN = {TABLE_IS_CTAS};

  /**
   * Get a Map of table or partition properties to be used in explain extended output.
   * Do some filtering to make output readable and/or concise.
   */
  static Map<Object, Object> getPropertiesForExplain(Properties properties) {
    if (properties != null) {
      Map<Object, Object> clone = null;
      String value = properties.getProperty("columns.comments");
      if (value != null) {
        // should copy properties first
        clone = new HashMap<>(properties);
        clone.put("columns.comments", quoteComments(value));
      }
      value = properties.getProperty(StatsSetupConst.NUM_ERASURE_CODED_FILES);
      if ("0".equals(value)) {
        // should copy properties first
        if (clone == null) {
          clone = new HashMap<>(properties);
        }
        clone.remove(StatsSetupConst.NUM_ERASURE_CODED_FILES);
      }
      for (String key : FILTER_OUT_FROM_EXPLAIN) {
        if (properties.containsKey(key)) {
          if (clone == null) {
            clone = new HashMap<>(properties);
          }
          clone.remove(key);
        }
      }
      if (clone != null) {
        return clone;
      }
    }
    return properties;
  }

  public static Map<String, String> getPropertiesForExplain(Map<String, String> properties, String... propertiesToRemove) {
    if (properties == null) {
      return Collections.emptyMap();
    }

    Map<String, String> clone = new HashMap<>(properties);
    for (String key : propertiesToRemove) {
      clone.remove(key);
    }

    return clone;
  }
}
