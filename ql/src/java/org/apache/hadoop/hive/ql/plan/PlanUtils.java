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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.DelimitedJSONSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * PlanUtils.
 *
 */
public final class PlanUtils {

  protected static final Log LOG = LogFactory.getLog("org.apache.hadoop.hive.ql.plan.PlanUtils");

  private static long countForMapJoinDumpFilePrefix = 0;

  /**
   * ExpressionTypes.
   *
   */
  public static enum ExpressionTypes {
    FIELD, JEXL
  };

  public static long getCountForMapJoinDumpFilePrefix() {
    return countForMapJoinDumpFilePrefix++;
  }

  @SuppressWarnings("nls")
  public static MapredWork getMapRedWork() {
    try {
      return new MapredWork("", new LinkedHashMap<String, ArrayList<String>>(),
        new LinkedHashMap<String, PartitionDesc>(),
        new LinkedHashMap<String, Operator<? extends OperatorDesc>>(),
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
      boolean useDelimitedJSON) {

    return getTableDesc(serdeClass, separatorCode, columns, columnTypes,
        lastColumnTakesRestOfTheLine, useDelimitedJSON, "TextFile");
 }

  public static TableDesc getTableDesc(
      Class<? extends Deserializer> serdeClass, String separatorCode,
      String columns, String columnTypes, boolean lastColumnTakesRestOfTheLine,
      boolean useDelimitedJSON, String fileFormat) {

    Properties properties = Utilities.makeProperties(
        serdeConstants.SERIALIZATION_FORMAT, separatorCode, serdeConstants.LIST_COLUMNS,
        columns);

    if (!separatorCode.equals(Integer.toString(Utilities.ctrlaCode))) {
      properties.setProperty(serdeConstants.FIELD_DELIM, separatorCode);
    }

    if (columnTypes != null) {
      properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, columnTypes);
    }

    if (lastColumnTakesRestOfTheLine) {
      properties.setProperty(serdeConstants.SERIALIZATION_LAST_COLUMN_TAKES_REST,
          "true");
    }

    // It is not a very clean way, and should be modified later - due to
    // compatiblity reasons,
    // user sees the results as json for custom scripts and has no way for
    // specifying that.
    // Right now, it is hard-coded in the code
    if (useDelimitedJSON) {
      serdeClass = DelimitedJSONSerDe.class;
    }

    Class inputFormat, outputFormat;
    // get the input & output file formats
    if ("SequenceFile".equalsIgnoreCase(fileFormat)) {
      inputFormat = SequenceFileInputFormat.class;
      outputFormat = SequenceFileOutputFormat.class;
    } else if ("RCFile".equalsIgnoreCase(fileFormat)) {
      inputFormat = RCFileInputFormat.class;
      outputFormat = RCFileOutputFormat.class;
      assert serdeClass == ColumnarSerDe.class;
    } else { // use TextFile by default
      inputFormat = TextInputFormat.class;
      outputFormat = IgnoreKeyTextOutputFormat.class;
    }
    return new TableDesc(serdeClass, inputFormat, outputFormat, properties);
  }

  public static TableDesc getDefaultQueryOutputTableDesc(String cols, String colTypes,
      String fileFormat) {
    TableDesc tblDesc = getTableDesc(LazySimpleSerDe.class, "" + Utilities.ctrlaCode, cols, colTypes,
        false, false, fileFormat);
    tblDesc.getProperties().setProperty(serdeConstants.ESCAPE_CHAR, "\\");
    return tblDesc;
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

      if (crtTblDesc.getTableName() != null && crtTblDesc.getDatabaseName() != null) {
        properties.setProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_NAME,
            crtTblDesc.getDatabaseName() + "." + crtTblDesc.getTableName());
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
        org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT,
        separatorCode));
  }

  /**
   * Generate the table descriptor for reduce key.
   */
  public static TableDesc getReduceKeyTableDesc(List<FieldSchema> fieldSchemas,
      String order) {
    return new TableDesc(BinarySortableSerDe.class,
        SequenceFileInputFormat.class, SequenceFileOutputFormat.class,
        Utilities.makeProperties(serdeConstants.LIST_COLUMNS, MetaStoreUtils
        .getColumnNamesFromFieldSchema(fieldSchemas),
        serdeConstants.LIST_COLUMN_TYPES, MetaStoreUtils
        .getColumnTypesFromFieldSchema(fieldSchemas),
        serdeConstants.SERIALIZATION_SORT_ORDER, order));
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
        serdeConstants.ESCAPE_CHAR, "\\"));
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
        serdeConstants.ESCAPE_CHAR, "\\"));
  }

  /**
   * Generate the table descriptor for intermediate files.
   */
  public static TableDesc getIntermediateFileTableDesc(
      List<FieldSchema> fieldSchemas) {
    return new TableDesc(LazyBinarySerDe.class, SequenceFileInputFormat.class,
        SequenceFileOutputFormat.class, Utilities.makeProperties(
        serdeConstants.LIST_COLUMNS, MetaStoreUtils
        .getColumnNamesFromFieldSchema(fieldSchemas),
        serdeConstants.LIST_COLUMN_TYPES, MetaStoreUtils
        .getColumnTypesFromFieldSchema(fieldSchemas),
        serdeConstants.ESCAPE_CHAR, "\\"));
  }

  /**
   * Generate the table descriptor for intermediate files.
   */
  public static TableDesc getReduceValueTableDesc(List<FieldSchema> fieldSchemas) {
    return new TableDesc(LazyBinarySerDe.class, SequenceFileInputFormat.class,
        SequenceFileOutputFormat.class, Utilities.makeProperties(
        serdeConstants.LIST_COLUMNS, MetaStoreUtils
        .getColumnNamesFromFieldSchema(fieldSchemas),
        serdeConstants.LIST_COLUMN_TYPES, MetaStoreUtils
        .getColumnTypesFromFieldSchema(fieldSchemas),
        serdeConstants.ESCAPE_CHAR, "\\"));
  }

  /**
   * Convert the ColumnList to FieldSchema list.
   *
   * Adds uniontype for distinctColIndices.
   */
  public static List<FieldSchema> getFieldSchemasFromColumnListWithLength(
      List<ExprNodeDesc> cols, List<List<Integer>> distinctColIndices,
      List<String> outputColumnNames, int length,
      String fieldPrefix) {
    // last one for union column.
    List<FieldSchema> schemas = new ArrayList<FieldSchema>(length + 1);
    for (int i = 0; i < length; i++) {
      schemas.add(MetaStoreUtils.getFieldSchemaFromTypeInfo(
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
      schemas.add(MetaStoreUtils.getFieldSchemaFromTypeInfo(
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
    return getReduceSinkDesc(keyCols, keyCols.size(), valueCols,
        new ArrayList<List<Integer>>(),
        includeKeyCols ? outputColumnNames.subList(0, keyCols.size()) :
          new ArrayList<String>(),
        includeKeyCols ? outputColumnNames.subList(keyCols.size(),
            outputColumnNames.size()) : outputColumnNames,
        includeKeyCols, tag, partitionCols, order, numReducers);
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
   * @return The reduceSinkDesc object.
   */
  public static ReduceSinkDesc getReduceSinkDesc(
      final ArrayList<ExprNodeDesc> keyCols, int numKeys,
      ArrayList<ExprNodeDesc> valueCols,
      List<List<Integer>> distinctColIndices,
      List<String> outputKeyColumnNames,
      List<String> outputValueColumnNames,
      boolean includeKeyCols, int tag,
      ArrayList<ExprNodeDesc> partitionCols, String order, int numReducers) {
    TableDesc keyTable = null;
    TableDesc valueTable = null;
    ArrayList<String> outputKeyCols = new ArrayList<String>();
    ArrayList<String> outputValCols = new ArrayList<String>();
    if (includeKeyCols) {
      List<FieldSchema> keySchema = getFieldSchemasFromColumnListWithLength(
          keyCols, distinctColIndices, outputKeyColumnNames, numKeys, "");
      if (order.length() < outputKeyColumnNames.size()) {
        order = order + "+";
      }
      keyTable = getReduceKeyTableDesc(keySchema, order);
      outputKeyCols.addAll(outputKeyColumnNames);
    } else {
      keyTable = getReduceKeyTableDesc(getFieldSchemasFromColumnList(
          keyCols, "reducesinkkey"),order);
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
    return getReduceSinkDesc(keyCols, keyCols.size(), valueCols,
        new ArrayList<List<Integer>>(),
        includeKey ? outputColumnNames.subList(0, keyCols.size()) :
          new ArrayList<String>(),
        includeKey ?
            outputColumnNames.subList(keyCols.size(), outputColumnNames.size())
            : outputColumnNames,
        includeKey, tag, numPartitionFields, numReducers);
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
   * @return The reduceSinkDesc object.
   */
  public static ReduceSinkDesc getReduceSinkDesc(
      ArrayList<ExprNodeDesc> keyCols, int numKeys,
      ArrayList<ExprNodeDesc> valueCols,
      List<List<Integer>> distinctColIndices,
      List<String> outputKeyColumnNames, List<String> outputValueColumnNames,
      boolean includeKey, int tag,
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
    return getReduceSinkDesc(keyCols, numKeys, valueCols, distinctColIndices,
        outputKeyColumnNames, outputValueColumnNames, includeKey, tag,
        partitionCols, order.toString(), numReducers);
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
        if(input) {
            try {
                storageHandler.configureInputJobProperties(
                  tableDesc,
                  jobProperties);
            } catch(AbstractMethodError e) {
                LOG.debug("configureInputJobProperties not found "+
                    "using configureTableJobProperties",e);
                storageHandler.configureTableJobProperties(tableDesc, jobProperties);
            }
        }
        else {
            try {
                storageHandler.configureOutputJobProperties(
                  tableDesc,
                  jobProperties);
            } catch(AbstractMethodError e) {
                LOG.debug("configureOutputJobProperties not found"+
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
      }
    } catch (HiveException ex) {
      throw new RuntimeException(ex);
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
   * Remove prefix from "Path -> Alias"
   * This is required for testing.
   * In order to verify that path is right, we need to display it in expected test result.
   * But, mask pattern masks path with some patterns.
   * So, we need to remove prefix from path which triggers mask pattern.
   * @param origiKey
   * @return
   */
  public static String removePrefixFromWarehouseConfig(String origiKey) {
    String prefix = SessionState.get().getConf().getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);
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

  // Add the input 'newInput' to the set of inputs for the query.
  // The input may or may not be already present.
  // The ReadEntity also contains the parents from it is derived (only populated
  // in case of views). The equals method for ReadEntity does not compare the parents
  // so that the same input with different parents cannot be added twice. If the input
  // is already present, make sure the parents are added.
  // Consider the query:
  // select * from (select * from V2 union all select * from V3) subq;
  // where both V2 and V3 depend on V1
  // addInput would be called twice for V1 (one with parent V2 and the other with parent V3).
  // When addInput is called for the first time for V1, V1 (parent V2) is added to inputs.
  // When addInput is called for the second time for V1, the input V1 from inputs is picked up,
  // and it's parents are enhanced to include V2 and V3
  // The inputs will contain: (V2, no parent), (V3, no parent), (v1, parents(V2, v3))
  public static ReadEntity addInput(Set<ReadEntity> inputs, ReadEntity newInput) {
    // If the input is already present, make sure the new parent is added to the input.
    if (inputs.contains(newInput)) {
      for (ReadEntity input : inputs) {
        if (input.equals(newInput)) {
          if ((newInput.getParents() != null) && (!newInput.getParents().isEmpty())) {
            input.getParents().addAll(newInput.getParents());
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
}
