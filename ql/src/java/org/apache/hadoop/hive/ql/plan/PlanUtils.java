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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * PlanUtils.
 *
 */
public final class PlanUtils {

  protected static final Log LOG = LogFactory.getLog("org.apache.hadoop.hive.ql.plan.PlanUtils");

  /**
   * ExpressionTypes.
   *
   */
  public static enum ExpressionTypes {
    FIELD, JEXL
  };

  @SuppressWarnings("nls")
  public static MapredWork getMapRedWork() {
    try {
      return new MapredWork("", new LinkedHashMap<String, ArrayList<String>>(),
        new LinkedHashMap<String, PartitionDesc>(),
        new LinkedHashMap<String, Operator<? extends Serializable>>(),
        new TableDesc(), new ArrayList<TableDesc>(), null, Integer.valueOf(1),
        null, Hive.get().getConf().getBoolVar(
          HiveConf.ConfVars.HIVE_COMBINE_INPUT_FORMAT_SUPPORTS_SPLITTABLE));
    } catch (HiveException ex) {
      throw new RuntimeException(ex);
    }
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
    return getTableDesc(serdeClass, separatorCode, columns, null,
        lastColumnTakesRestOfTheLine);
  }

  /**
   * Generate the table descriptor of MetadataTypedColumnsetSerDe with the
   * separatorCode and column names (comma separated string), and whether the
   * last column should take the rest of the line.
   */
  public static TableDesc getDefaultTableDesc(String separatorCode,
      String columns, String columnTypes, boolean lastColumnTakesRestOfTheLine) {
    return getTableDesc(LazySimpleSerDe.class, separatorCode, columns,
        columnTypes, lastColumnTakesRestOfTheLine);
  }

  public static TableDesc getTableDesc(
      Class<? extends Deserializer> serdeClass, String separatorCode,
      String columns, String columnTypes, boolean lastColumnTakesRestOfTheLine) {
    return getTableDesc(serdeClass, separatorCode, columns, columnTypes,
        lastColumnTakesRestOfTheLine, false);
  }

  public static TableDesc getTableDesc(
      Class<? extends Deserializer> serdeClass, String separatorCode,
      String columns, String columnTypes, boolean lastColumnTakesRestOfTheLine,
      boolean useJSONForLazy) {

    Properties properties = Utilities.makeProperties(
        Constants.SERIALIZATION_FORMAT, separatorCode, Constants.LIST_COLUMNS,
        columns);

    if (!separatorCode.equals(Integer.toString(Utilities.ctrlaCode))) {
      properties.setProperty(Constants.FIELD_DELIM, separatorCode);
    }

    if (columnTypes != null) {
      properties.setProperty(Constants.LIST_COLUMN_TYPES, columnTypes);
    }

    if (lastColumnTakesRestOfTheLine) {
      properties.setProperty(Constants.SERIALIZATION_LAST_COLUMN_TAKES_REST,
          "true");
    }

    // It is not a very clean way, and should be modified later - due to
    // compatiblity reasons,
    // user sees the results as json for custom scripts and has no way for
    // specifying that.
    // Right now, it is hard-coded in the code
    if (useJSONForLazy) {
      properties.setProperty(Constants.SERIALIZATION_USE_JSON_OBJECTS, "true");
    }

    return new TableDesc(serdeClass, TextInputFormat.class,
        IgnoreKeyTextOutputFormat.class, properties);
  }

  /**
   * Generate a table descriptor from a createTableDesc.
   */
  public static TableDesc getTableDesc(CreateTableDesc crtTblDesc, String cols,
      String colTypes) {

    Class<? extends Deserializer> serdeClass = LazySimpleSerDe.class;
    String separatorCode = Integer.toString(Utilities.ctrlaCode);
    String columns = cols;
    String columnTypes = colTypes;
    boolean lastColumnTakesRestOfTheLine = false;
    TableDesc ret;

    try {
      if (crtTblDesc.getSerName() != null) {
        Class c = Class.forName(crtTblDesc.getSerName());
        serdeClass = c;
      }

      if (crtTblDesc.getFieldDelim() != null) {
        separatorCode = crtTblDesc.getFieldDelim();
      }

      ret = getTableDesc(serdeClass, separatorCode, columns, columnTypes,
          lastColumnTakesRestOfTheLine, false);

      // set other table properties
      Properties properties = ret.getProperties();

      if (crtTblDesc.getCollItemDelim() != null) {
        properties.setProperty(Constants.COLLECTION_DELIM, crtTblDesc
            .getCollItemDelim());
      }

      if (crtTblDesc.getMapKeyDelim() != null) {
        properties.setProperty(Constants.MAPKEY_DELIM, crtTblDesc
            .getMapKeyDelim());
      }

      if (crtTblDesc.getFieldEscape() != null) {
        properties.setProperty(Constants.ESCAPE_CHAR, crtTblDesc
            .getFieldEscape());
      }

      if (crtTblDesc.getLineDelim() != null) {
        properties.setProperty(Constants.LINE_DELIM, crtTblDesc.getLineDelim());
      }

      // replace the default input & output file format with those found in
      // crtTblDesc
      Class c1 = Class.forName(crtTblDesc.getInputFormat());
      Class c2 = Class.forName(crtTblDesc.getOutputFormat());
      Class<? extends InputFormat> in_class = c1;
      Class<? extends HiveOutputFormat> out_class = c2;

      ret.setInputFileFormatClass(in_class);
      ret.setOutputFileFormatClass(out_class);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      return null;
    }
    return ret;
  }

  /**
   * Generate the table descriptor of MetadataTypedColumnsetSerDe with the
   * separatorCode. MetaDataTypedColumnsetSerDe is used because LazySimpleSerDe
   * does not support a table with a single column "col" with type
   * "array<string>".
   */
  public static TableDesc getDefaultTableDesc(String separatorCode) {
    return new TableDesc(MetadataTypedColumnsetSerDe.class,
        TextInputFormat.class, IgnoreKeyTextOutputFormat.class, Utilities
        .makeProperties(
        org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT,
        separatorCode));
  }

  /**
   * Generate the table descriptor for reduce key.
   */
  public static TableDesc getReduceKeyTableDesc(List<FieldSchema> fieldSchemas,
      String order) {
    return new TableDesc(BinarySortableSerDe.class,
        SequenceFileInputFormat.class, SequenceFileOutputFormat.class,
        Utilities.makeProperties(Constants.LIST_COLUMNS, MetaStoreUtils
        .getColumnNamesFromFieldSchema(fieldSchemas),
        Constants.LIST_COLUMN_TYPES, MetaStoreUtils
        .getColumnTypesFromFieldSchema(fieldSchemas),
        Constants.SERIALIZATION_SORT_ORDER, order));
  }

  /**
   * Generate the table descriptor for Map-side join key.
   */
  public static TableDesc getMapJoinKeyTableDesc(List<FieldSchema> fieldSchemas) {
    return new TableDesc(LazyBinarySerDe.class, SequenceFileInputFormat.class,
        SequenceFileOutputFormat.class, Utilities.makeProperties("columns",
        MetaStoreUtils.getColumnNamesFromFieldSchema(fieldSchemas),
        "columns.types", MetaStoreUtils
        .getColumnTypesFromFieldSchema(fieldSchemas),
        Constants.ESCAPE_CHAR, "\\"));
  }

  /**
   * Generate the table descriptor for Map-side join key.
   */
  public static TableDesc getMapJoinValueTableDesc(
      List<FieldSchema> fieldSchemas) {
    return new TableDesc(LazyBinarySerDe.class, SequenceFileInputFormat.class,
        SequenceFileOutputFormat.class, Utilities.makeProperties("columns",
        MetaStoreUtils.getColumnNamesFromFieldSchema(fieldSchemas),
        "columns.types", MetaStoreUtils
        .getColumnTypesFromFieldSchema(fieldSchemas),
        Constants.ESCAPE_CHAR, "\\"));
  }

  /**
   * Generate the table descriptor for intermediate files.
   */
  public static TableDesc getIntermediateFileTableDesc(
      List<FieldSchema> fieldSchemas) {
    return new TableDesc(LazyBinarySerDe.class, SequenceFileInputFormat.class,
        SequenceFileOutputFormat.class, Utilities.makeProperties(
        Constants.LIST_COLUMNS, MetaStoreUtils
        .getColumnNamesFromFieldSchema(fieldSchemas),
        Constants.LIST_COLUMN_TYPES, MetaStoreUtils
        .getColumnTypesFromFieldSchema(fieldSchemas),
        Constants.ESCAPE_CHAR, "\\"));
  }

  /**
   * Generate the table descriptor for intermediate files.
   */
  public static TableDesc getReduceValueTableDesc(List<FieldSchema> fieldSchemas) {
    return new TableDesc(LazyBinarySerDe.class, SequenceFileInputFormat.class,
        SequenceFileOutputFormat.class, Utilities.makeProperties(
        Constants.LIST_COLUMNS, MetaStoreUtils
        .getColumnNamesFromFieldSchema(fieldSchemas),
        Constants.LIST_COLUMN_TYPES, MetaStoreUtils
        .getColumnTypesFromFieldSchema(fieldSchemas),
        Constants.ESCAPE_CHAR, "\\"));
  }

  /**
   * Convert the ColumnList to FieldSchema list.
   */
  public static List<FieldSchema> getFieldSchemasFromColumnList(
      List<ExprNodeDesc> cols, List<String> outputColumnNames, int start,
      String fieldPrefix) {
    List<FieldSchema> schemas = new ArrayList<FieldSchema>(cols.size());
    for (int i = 0; i < cols.size(); i++) {
      schemas.add(MetaStoreUtils.getFieldSchemaFromTypeInfo(fieldPrefix
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
      schemas.add(MetaStoreUtils.getFieldSchemaFromTypeInfo(fieldPrefix + i,
          cols.get(i).getTypeInfo()));
    }
    return schemas;
  }

  /**
   * Convert the RowSchema to FieldSchema list.
   */
  public static List<FieldSchema> getFieldSchemasFromRowSchema(RowSchema row,
      String fieldPrefix) {
    ArrayList<ColumnInfo> c = row.getSignature();
    return getFieldSchemasFromColumnInfo(c, fieldPrefix);
  }

  /**
   * Convert the ColumnInfo to FieldSchema.
   */
  public static List<FieldSchema> getFieldSchemasFromColumnInfo(
      ArrayList<ColumnInfo> cols, String fieldPrefix) {
    if ((cols == null) || (cols.size() == 0)) {
      return new ArrayList<FieldSchema>();
    }

    List<FieldSchema> schemas = new ArrayList<FieldSchema>(cols.size());
    for (int i = 0; i < cols.size(); i++) {
      String name = cols.get(i).getInternalName();
      if (name.equals(Integer.valueOf(i).toString())) {
        name = fieldPrefix + name;
      }
      schemas.add(MetaStoreUtils.getFieldSchemaFromTypeInfo(name, cols.get(i)
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
   * @return The reduceSinkDesc object.
   */
  public static ReduceSinkDesc getReduceSinkDesc(
      ArrayList<ExprNodeDesc> keyCols, ArrayList<ExprNodeDesc> valueCols,
      List<String> outputColumnNames, boolean includeKeyCols, int tag,
      ArrayList<ExprNodeDesc> partitionCols, String order, int numReducers) {
    TableDesc keyTable = null;
    TableDesc valueTable = null;
    ArrayList<String> outputKeyCols = new ArrayList<String>();
    ArrayList<String> outputValCols = new ArrayList<String>();
    if (includeKeyCols) {
      keyTable = getReduceKeyTableDesc(getFieldSchemasFromColumnList(keyCols,
          outputColumnNames, 0, ""), order);
      outputKeyCols.addAll(outputColumnNames.subList(0, keyCols.size()));
      valueTable = getReduceValueTableDesc(getFieldSchemasFromColumnList(
          valueCols, outputColumnNames, keyCols.size(), ""));
      outputValCols.addAll(outputColumnNames.subList(keyCols.size(),
          outputColumnNames.size()));
    } else {
      keyTable = getReduceKeyTableDesc(getFieldSchemasFromColumnList(keyCols,
          "reducesinkkey"), order);
      for (int i = 0; i < keyCols.size(); i++) {
        outputKeyCols.add("reducesinkkey" + i);
      }
      valueTable = getReduceValueTableDesc(getFieldSchemasFromColumnList(
          valueCols, outputColumnNames, 0, ""));
      outputValCols.addAll(outputColumnNames);
    }
    return new ReduceSinkDesc(keyCols, valueCols, outputKeyCols, outputValCols,
        tag, partitionCols, numReducers, keyTable,
            // Revert to DynamicSerDe:
        // getBinaryTableDesc(getFieldSchemasFromColumnList(valueCols,
        // "reducesinkvalue")));
        valueTable);
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
   * @return The reduceSinkDesc object.
   */
  public static ReduceSinkDesc getReduceSinkDesc(
      ArrayList<ExprNodeDesc> keyCols, ArrayList<ExprNodeDesc> valueCols,
      List<String> outputColumnNames, boolean includeKey, int tag,
      int numPartitionFields, int numReducers) throws SemanticException {
    ArrayList<ExprNodeDesc> partitionCols = null;

    if (numPartitionFields >= keyCols.size()) {
      partitionCols = keyCols;
    } else if (numPartitionFields >= 0) {
      partitionCols = new ArrayList<ExprNodeDesc>(numPartitionFields);
      for (int i = 0; i < numPartitionFields; i++) {
        partitionCols.add(keyCols.get(i));
      }
    } else {
      // numPartitionFields = -1 means random partitioning
      partitionCols = new ArrayList<ExprNodeDesc>(1);
      partitionCols.add(TypeCheckProcFactory.DefaultExprProcessor
          .getFuncExprNodeDesc("rand"));
    }

    StringBuilder order = new StringBuilder();
    for (int i = 0; i < keyCols.size(); i++) {
      order.append("+");
    }
    return getReduceSinkDesc(keyCols, valueCols, outputColumnNames, includeKey,
        tag, partitionCols, order.toString(), numReducers);
  }

  /**
   * Loads the storage handler (if one exists) for the given table
   * and invokes {@link HiveStorageHandler#configureTableJobProperties}.
   *
   * @param tableDesc table descriptor
   */
  public static void configureTableJobPropertiesForStorageHandler(
    TableDesc tableDesc) {

    if (tableDesc == null) {
      return;
    }

    try {
      HiveStorageHandler storageHandler =
        HiveUtils.getStorageHandler(
          Hive.get().getConf(),
          tableDesc.getProperties().getProperty(
            org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_STORAGE));
      if (storageHandler != null) {
        Map<String, String> jobProperties = new LinkedHashMap<String, String>();
        storageHandler.configureTableJobProperties(
          tableDesc,
          jobProperties);
        // Job properties are only relevant for non-native tables, so
        // for native tables, leave it null to avoid cluttering up
        // plans.
        if (!jobProperties.isEmpty()) {
          tableDesc.setJobProperties(jobProperties);
        }
      }
    } catch (HiveException ex) {
      throw new RuntimeException(ex);
    }
  }

  private PlanUtils() {
    // prevent instantiation
  }

}
