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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Predicates;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hive.common.util.ReflectionUtil;

public class MetaStoreUtils {

  private static final Logger LOG = LoggerFactory.getLogger("hive.log");

  // Right now we only support one special character '/'.
  // More special characters can be added accordingly in the future.
  // NOTE:
  // If the following array is updated, please also be sure to update the
  // configuration parameter documentation
  // HIVE_SUPPORT_SPECICAL_CHARACTERS_IN_TABLE_NAMES in HiveConf as well.
  public static final char[] specialCharactersInTableNames = new char[] { '/' };

  public static void populateQuickStats(FileStatus[] fileStatus, Map<String, String> params) {
    org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.populateQuickStats(fileStatus, params);
  }

  public static boolean updateTableStatsFast(Table tbl, FileStatus[] fileStatus, boolean newDir,
                                             boolean forceRecompute, EnvironmentContext environmentContext) throws MetaException {
    return org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.updateTableStatsFast(
        tbl, fileStatus, newDir, forceRecompute, environmentContext);
  }

  public static boolean updatePartitionStatsFast(Partition part, Warehouse wh, EnvironmentContext environmentContext)
      throws MetaException {
    return org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.updatePartitionStatsFast(
        part, wh, environmentContext);
  }

  /**
   * getDeserializer
   *
   * Get the Deserializer for a table.
   *
   * @param conf
   *          - hadoop config
   * @param table
   *          the table
   * @return
   *   Returns instantiated deserializer by looking up class name of deserializer stored in
   *   storage descriptor of passed in table. Also, initializes the deserializer with schema
   *   of table.
   * @exception MetaException
   *              if any problems instantiating the Deserializer
   *
   *              todo - this should move somewhere into serde.jar
   *
   */
  static public Deserializer getDeserializer(Configuration conf,
      org.apache.hadoop.hive.metastore.api.Table table, boolean skipConfError) throws
          MetaException {
    String lib = table.getSd().getSerdeInfo().getSerializationLib();
    if (lib == null) {
      return null;
    }
    return getDeserializer(conf, table, skipConfError, lib);
  }

  public static Deserializer getDeserializer(Configuration conf,
      org.apache.hadoop.hive.metastore.api.Table table, boolean skipConfError,
      String lib) throws MetaException {
    try {
      Deserializer deserializer = ReflectionUtil.newInstance(conf.getClassByName(lib).
              asSubclass(Deserializer.class), conf);
      if (skipConfError) {
        SerDeUtils.initializeSerDeWithoutErrorCheck(deserializer, conf,
                MetaStoreUtils.getTableMetadata(table), null);
      } else {
        SerDeUtils.initializeSerDe(deserializer, conf, MetaStoreUtils.getTableMetadata(table), null);
      }
      return deserializer;
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      LOG.error("error in initSerDe: " + e.getClass().getName() + " "
          + e.getMessage(), e);
      throw new MetaException(e.getClass().getName() + " " + e.getMessage());
    }
  }

  public static Class<? extends Deserializer> getDeserializerClass(
      Configuration conf, org.apache.hadoop.hive.metastore.api.Table table) throws Exception {
    String lib = table.getSd().getSerdeInfo().getSerializationLib();
    return lib == null ? null : conf.getClassByName(lib).asSubclass(Deserializer.class);
  }

  /**
   * getDeserializer
   *
   * Get the Deserializer for a partition.
   *
   * @param conf
   *          - hadoop config
   * @param part
   *          the partition
   * @param table the table
   * @return
   *   Returns instantiated deserializer by looking up class name of deserializer stored in
   *   storage descriptor of passed in partition. Also, initializes the deserializer with
   *   schema of partition.
   * @exception MetaException
   *              if any problems instantiating the Deserializer
   *
   */
  static public Deserializer getDeserializer(Configuration conf,
      org.apache.hadoop.hive.metastore.api.Partition part,
      org.apache.hadoop.hive.metastore.api.Table table) throws MetaException {
    String lib = part.getSd().getSerdeInfo().getSerializationLib();
    try {
      Deserializer deserializer = ReflectionUtil.newInstance(conf.getClassByName(lib).
        asSubclass(Deserializer.class), conf);
      SerDeUtils.initializeSerDe(deserializer, conf, MetaStoreUtils.getTableMetadata(table),
                                 MetaStoreUtils.getPartitionMetadata(part, table));
      return deserializer;
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      LOG.error("error in initSerDe: " + e.getClass().getName() + " "
          + e.getMessage(), e);
      throw new MetaException(e.getClass().getName() + " " + e.getMessage());
    }
  }

  /**
   * Given a list of partition columns and a partial mapping from
   * some partition columns to values the function returns the values
   * for the column.
   * @param partCols the list of table partition columns
   * @param partSpec the partial mapping from partition column to values
   * @return list of values of for given partition columns, any missing
   *         values in partSpec is replaced by an empty string
   */
  public static List<String> getPvals(List<FieldSchema> partCols,
      Map<String, String> partSpec) {
    List<String> pvals = new ArrayList<String>(partCols.size());
    for (FieldSchema field : partCols) {
      String val = StringUtils.defaultString(partSpec.get(field.getName()));
      pvals.add(val);
    }
    return pvals;
  }

  /**
   * validateName
   *
   * Checks the name conforms to our standars which are: "[a-zA-z_0-9]+". checks
   * this is just characters and numbers and _
   *
   * @param name
   *          the name to validate
   * @param conf
   *          hive configuration
   * @return true or false depending on conformance
   *              if it doesn't match the pattern.
   */
  static public boolean validateName(String name, Configuration conf) {
    Pattern tpat = null;
    String allowedCharacters = "\\w_";
    if (conf != null
        && HiveConf.getBoolVar(conf,
            HiveConf.ConfVars.HIVE_SUPPORT_SPECICAL_CHARACTERS_IN_TABLE_NAMES)) {
      for (Character c : specialCharactersInTableNames) {
        allowedCharacters += c;
      }
    }
    tpat = Pattern.compile("[" + allowedCharacters + "]+");
    Matcher m = tpat.matcher(name);
    return m.matches();
  }

  /*
   * At the Metadata level there are no restrictions on Column Names.
   */
  public static boolean validateColumnName(String name) {
    return true;
  }

  public static final String TYPE_FROM_DESERIALIZER = "<derived from deserializer>";

  public static String getListType(String t) {
    return "array<" + t + ">";
  }

  static HashMap<String, String> typeToThriftTypeMap;
  static {
    typeToThriftTypeMap = new HashMap<String, String>();
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.BOOLEAN_TYPE_NAME, "bool");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.TINYINT_TYPE_NAME, "byte");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.SMALLINT_TYPE_NAME, "i16");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.INT_TYPE_NAME, "i32");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.BIGINT_TYPE_NAME, "i64");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.DOUBLE_TYPE_NAME, "double");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.FLOAT_TYPE_NAME, "float");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.LIST_TYPE_NAME, "list");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.MAP_TYPE_NAME, "map");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME, "string");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.BINARY_TYPE_NAME, "binary");
    // These 4 types are not supported yet.
    // We should define a complex type date in thrift that contains a single int
    // member, and DynamicSerDe
    // should convert it to date type at runtime.
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.DATE_TYPE_NAME, "date");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.DATETIME_TYPE_NAME, "datetime");
    typeToThriftTypeMap
        .put(org.apache.hadoop.hive.serde.serdeConstants.TIMESTAMP_TYPE_NAME,
            "timestamp");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.DECIMAL_TYPE_NAME, "decimal");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME,
        org.apache.hadoop.hive.serde.serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME);
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME,
        org.apache.hadoop.hive.serde.serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME);
  }

  private static Set<String> hiveThriftTypeMap; //for validation
  static {
    hiveThriftTypeMap = new HashSet<String>();
    hiveThriftTypeMap.addAll(serdeConstants.PrimitiveTypes);
    hiveThriftTypeMap.addAll(org.apache.hadoop.hive.serde.serdeConstants.CollectionTypes);
    hiveThriftTypeMap.add(org.apache.hadoop.hive.serde.serdeConstants.UNION_TYPE_NAME);
    hiveThriftTypeMap.add(org.apache.hadoop.hive.serde.serdeConstants.STRUCT_TYPE_NAME);
  }

  /**
   * Convert type to ThriftType. We do that by tokenizing the type and convert
   * each token.
   */
  public static String typeToThriftType(String type) {
    StringBuilder thriftType = new StringBuilder();
    int last = 0;
    boolean lastAlphaDigit = Character.isLetterOrDigit(type.charAt(last));
    for (int i = 1; i <= type.length(); i++) {
      if (i == type.length()
          || Character.isLetterOrDigit(type.charAt(i)) != lastAlphaDigit) {
        String token = type.substring(last, i);
        last = i;
        String thriftToken = typeToThriftTypeMap.get(token);
        thriftType.append(thriftToken == null ? token : thriftToken);
        lastAlphaDigit = !lastAlphaDigit;
      }
    }
    return thriftType.toString();
  }

  /**
   * Convert FieldSchemas to Thrift DDL.
   */
  public static String getDDLFromFieldSchema(String structName,
      List<FieldSchema> fieldSchemas) {
    StringBuilder ddl = new StringBuilder();
    ddl.append("struct ");
    ddl.append(structName);
    ddl.append(" { ");
    boolean first = true;
    for (FieldSchema col : fieldSchemas) {
      if (first) {
        first = false;
      } else {
        ddl.append(", ");
      }
      ddl.append(typeToThriftType(col.getType()));
      ddl.append(' ');
      ddl.append(col.getName());
    }
    ddl.append("}");

    LOG.trace("DDL: {}", ddl);
    return ddl.toString();
  }

  public static Properties getTableMetadata(
      org.apache.hadoop.hive.metastore.api.Table table) {
    return MetaStoreUtils.getSchema(table.getSd(), table.getSd(), table
        .getParameters(), table.getDbName(), table.getTableName(), table.getPartitionKeys());
  }

  public static Properties getPartitionMetadata(
      org.apache.hadoop.hive.metastore.api.Partition partition,
      org.apache.hadoop.hive.metastore.api.Table table) {
    return MetaStoreUtils
        .getSchema(partition.getSd(), partition.getSd(), partition
            .getParameters(), table.getDbName(), table.getTableName(),
            table.getPartitionKeys());
  }

  public static Properties getSchema(
      org.apache.hadoop.hive.metastore.api.Partition part,
      org.apache.hadoop.hive.metastore.api.Table table) {
    return MetaStoreUtils.getSchema(part.getSd(), table.getSd(), table
        .getParameters(), table.getDbName(), table.getTableName(), table.getPartitionKeys());
  }

  /**
   * Get partition level schema from table level schema.
   * This function will use the same column names, column types and partition keys for
   * each partition Properties. Their values are copied from the table Properties. This
   * is mainly to save CPU and memory. CPU is saved because the first time the
   * StorageDescriptor column names are accessed, JDO needs to execute a SQL query to
   * retrieve the data. If we know the data will be the same as the table level schema
   * and they are immutable, we should just reuse the table level schema objects.
   *
   * @param sd The Partition level Storage Descriptor.
   * @param tblsd The Table level Storage Descriptor.
   * @param parameters partition level parameters
   * @param databaseName DB name
   * @param tableName table name
   * @param partitionKeys partition columns
   * @param tblSchema The table level schema from which this partition should be copied.
   * @return the properties
   */
  public static Properties getPartSchemaFromTableSchema(
      org.apache.hadoop.hive.metastore.api.StorageDescriptor sd,
      org.apache.hadoop.hive.metastore.api.StorageDescriptor tblsd,
      Map<String, String> parameters, String databaseName, String tableName,
      List<FieldSchema> partitionKeys,
      Properties tblSchema) {

    // Inherent most properties from table level schema and overwrite some properties
    // in the following code.
    // This is mainly for saving CPU and memory to reuse the column names, types and
    // partition columns in the table level schema.
    Properties schema = (Properties) tblSchema.clone();

    // InputFormat
    String inputFormat = sd.getInputFormat();
    if (inputFormat == null || inputFormat.length() == 0) {
      String tblInput =
        schema.getProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT);
      if (tblInput == null) {
        inputFormat = org.apache.hadoop.mapred.SequenceFileInputFormat.class.getName();
      } else {
        inputFormat = tblInput;
      }
    }
    schema.setProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT,
        inputFormat);

    // OutputFormat
    String outputFormat = sd.getOutputFormat();
    if (outputFormat == null || outputFormat.length() == 0) {
      String tblOutput =
        schema.getProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_OUTPUT_FORMAT);
      if (tblOutput == null) {
        outputFormat = org.apache.hadoop.mapred.SequenceFileOutputFormat.class.getName();
      } else {
        outputFormat = tblOutput;
      }
    }
    schema.setProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_OUTPUT_FORMAT,
        outputFormat);

    // Location
    if (sd.getLocation() != null) {
      schema.setProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_LOCATION,
          sd.getLocation());
    }

    // Bucket count
    schema.setProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.BUCKET_COUNT,
        Integer.toString(sd.getNumBuckets()));

    if (sd.getBucketCols() != null && sd.getBucketCols().size() > 0) {
      schema.setProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.BUCKET_FIELD_NAME,
          sd.getBucketCols().get(0));
    }

    // SerdeInfo
    if (sd.getSerdeInfo() != null) {

      // We should not update the following 3 values if SerDeInfo contains these.
      // This is to keep backward compatible with getSchema(), where these 3 keys
      // are updated after SerDeInfo properties got copied.
      String cols = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
      String colTypes = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
      String parts = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS;

      for (Map.Entry<String,String> param : sd.getSerdeInfo().getParameters().entrySet()) {
        String key = param.getKey();
        if (schema.get(key) != null &&
            (key.equals(cols) || key.equals(colTypes) || key.equals(parts))) {
          continue;
        }
        schema.put(key, (param.getValue() != null) ? param.getValue() : StringUtils.EMPTY);
      }

      if (sd.getSerdeInfo().getSerializationLib() != null) {
        schema.setProperty(org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB,
            sd.getSerdeInfo().getSerializationLib());
      }
    }

    // skipping columns since partition level field schemas are the same as table level's
    // skipping partition keys since it is the same as table level partition keys

    if (parameters != null) {
      for (Entry<String, String> e : parameters.entrySet()) {
        schema.setProperty(e.getKey(), e.getValue());
      }
    }

    return schema;
  }

  public static Properties addCols(Properties schema, List<FieldSchema> cols) {

    StringBuilder colNameBuf = new StringBuilder();
    StringBuilder colTypeBuf = new StringBuilder();
    StringBuilder colComment = new StringBuilder();

    boolean first = true;
    String columnNameDelimiter = getColumnNameDelimiter(cols);
    for (FieldSchema col : cols) {
      if (!first) {
        colNameBuf.append(columnNameDelimiter);
        colTypeBuf.append(":");
        colComment.append('\0');
      }
      colNameBuf.append(col.getName());
      colTypeBuf.append(col.getType());
      colComment.append((null != col.getComment()) ? col.getComment() : StringUtils.EMPTY);
      first = false;
    }
    schema.setProperty(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS,
        colNameBuf.toString());
    schema.setProperty(serdeConstants.COLUMN_NAME_DELIMITER, columnNameDelimiter);
    String colTypes = colTypeBuf.toString();
    schema.setProperty(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES,
        colTypes);
    schema.setProperty("columns.comments", colComment.toString());

    return schema;

  }

  public static Properties getSchemaWithoutCols(org.apache.hadoop.hive.metastore.api.StorageDescriptor sd,
      org.apache.hadoop.hive.metastore.api.StorageDescriptor tblsd,
      Map<String, String> parameters, String databaseName, String tableName,
      List<FieldSchema> partitionKeys) {
    Properties schema = new Properties();
    String inputFormat = sd.getInputFormat();
    if (inputFormat == null || inputFormat.length() == 0) {
      inputFormat = org.apache.hadoop.mapred.SequenceFileInputFormat.class
        .getName();
    }
    schema.setProperty(
      org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT,
      inputFormat);
    String outputFormat = sd.getOutputFormat();
    if (outputFormat == null || outputFormat.length() == 0) {
      outputFormat = org.apache.hadoop.mapred.SequenceFileOutputFormat.class
        .getName();
    }
    schema.setProperty(
      org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_OUTPUT_FORMAT,
      outputFormat);

    schema.setProperty(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_NAME,
        databaseName + "." + tableName);

    if (sd.getLocation() != null) {
      schema.setProperty(
          org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_LOCATION,
          sd.getLocation());
    }
    schema.setProperty(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.BUCKET_COUNT, Integer
            .toString(sd.getNumBuckets()));
    if (sd.getBucketCols() != null && sd.getBucketCols().size() > 0) {
      schema.setProperty(
          org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.BUCKET_FIELD_NAME, sd
              .getBucketCols().get(0));
    }
    if (sd.getSerdeInfo() != null) {
      for (Map.Entry<String,String> param : sd.getSerdeInfo().getParameters().entrySet()) {
        schema.put(param.getKey(), (param.getValue() != null) ? param.getValue() : StringUtils.EMPTY);
      }

      if (sd.getSerdeInfo().getSerializationLib() != null) {
        schema.setProperty(
            org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB, sd
                .getSerdeInfo().getSerializationLib());
      }
    }

    if (sd.getCols() != null) {
      schema.setProperty(
          org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_DDL,
          getDDLFromFieldSchema(tableName, sd.getCols()));
    }

    String partString = StringUtils.EMPTY;
    String partStringSep = StringUtils.EMPTY;
    String partTypesString = StringUtils.EMPTY;
    String partTypesStringSep = StringUtils.EMPTY;
    for (FieldSchema partKey : partitionKeys) {
      partString = partString.concat(partStringSep);
      partString = partString.concat(partKey.getName());
      partTypesString = partTypesString.concat(partTypesStringSep);
      partTypesString = partTypesString.concat(partKey.getType());
      if (partStringSep.length() == 0) {
        partStringSep = "/";
        partTypesStringSep = ":";
      }
    }
    if (partString.length() > 0) {
      schema
          .setProperty(
              org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS,
              partString);
      schema
      .setProperty(
          org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES,
          partTypesString);
    }

    if (parameters != null) {
      for (Entry<String, String> e : parameters.entrySet()) {
        // add non-null parameters to the schema
        if ( e.getValue() != null) {
          schema.setProperty(e.getKey(), e.getValue());
        }
      }
    }

    return schema;
  }

  public static Properties getSchema(
      org.apache.hadoop.hive.metastore.api.StorageDescriptor sd,
      org.apache.hadoop.hive.metastore.api.StorageDescriptor tblsd,
      Map<String, String> parameters, String databaseName, String tableName,
      List<FieldSchema> partitionKeys) {

    return addCols(getSchemaWithoutCols(sd, tblsd, parameters, databaseName, tableName, partitionKeys), tblsd.getCols());
  }

  public static String getColumnNameDelimiter(List<FieldSchema> fieldSchemas) {
    // we first take a look if any fieldSchemas contain COMMA
    for (int i = 0; i < fieldSchemas.size(); i++) {
      if (fieldSchemas.get(i).getName().contains(",")) {
        return String.valueOf(SerDeUtils.COLUMN_COMMENTS_DELIMITER);
      }
    }
    return String.valueOf(SerDeUtils.COMMA);
  }

  /**
   * Convert FieldSchemas to columnNames.
   */
  public static String getColumnNamesFromFieldSchema(List<FieldSchema> fieldSchemas) {
    String delimiter = getColumnNameDelimiter(fieldSchemas);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < fieldSchemas.size(); i++) {
      if (i > 0) {
        sb.append(delimiter);
      }
      sb.append(fieldSchemas.get(i).getName());
    }
    return sb.toString();
  }

  /**
   * Convert FieldSchemas to columnTypes.
   */
  public static String getColumnTypesFromFieldSchema(
      List<FieldSchema> fieldSchemas) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < fieldSchemas.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append(fieldSchemas.get(i).getType());
    }
    return sb.toString();
  }

  public static String getColumnCommentsFromFieldSchema(List<FieldSchema> fieldSchemas) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < fieldSchemas.size(); i++) {
      if (i > 0) {
        sb.append(SerDeUtils.COLUMN_COMMENTS_DELIMITER);
      }
      sb.append(fieldSchemas.get(i).getComment());
    }
    return sb.toString();
  }

  public static int startMetaStore() throws Exception {
    return startMetaStore(HadoopThriftAuthBridge.getBridge(), null);
  }

  public static int startMetaStore(final HadoopThriftAuthBridge bridge, HiveConf conf) throws Exception {
    int port = findFreePort();
    startMetaStore(port, bridge, conf);
    return port;
  }

  public static int startMetaStore(HiveConf conf) throws Exception {
    return startMetaStore(HadoopThriftAuthBridge.getBridge(), conf);
  }

  public static void startMetaStore(final int port, final HadoopThriftAuthBridge bridge) throws Exception {
    startMetaStore(port, bridge, null);
  }

  public static void startMetaStore(final int port,
      final HadoopThriftAuthBridge bridge, HiveConf hiveConf)
      throws Exception{
    if (hiveConf == null) {
      hiveConf = new HiveConf(HMSHandler.class);
    }
    final HiveConf finalHiveConf = hiveConf;
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          HiveMetaStore.startMetaStore(port, bridge, finalHiveConf);
        } catch (Throwable e) {
          LOG.error("Metastore Thrift Server threw an exception...",e);
        }
      }
    });
    thread.setDaemon(true);
    thread.start();
    loopUntilHMSReady(port);
  }

  /**
   * A simple connect test to make sure that the metastore is up
   * @throws Exception
   */
  private static void loopUntilHMSReady(int port) throws Exception {
    int retries = 0;
    Exception exc = null;
    while (true) {
      try {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(port), 5000);
        socket.close();
        return;
      } catch (Exception e) {
        if (retries++ > 60) { //give up
          exc = e;
          break;
        }
        Thread.sleep(1000);
      }
    }
    // something is preventing metastore from starting
    // print the stack from all threads for debugging purposes
    LOG.error("Unable to connect to metastore server: " + exc.getMessage());
    LOG.info("Printing all thread stack traces for debugging before throwing exception.");
    LOG.info(getAllThreadStacksAsString());
    throw exc;
  }

  private static String getAllThreadStacksAsString() {
    Map<Thread, StackTraceElement[]> threadStacks = Thread.getAllStackTraces();
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<Thread, StackTraceElement[]> entry : threadStacks.entrySet()) {
      Thread t = entry.getKey();
      sb.append(System.lineSeparator());
      sb.append("Name: ").append(t.getName()).append(" State: ").append(t.getState());
      addStackString(entry.getValue(), sb);
    }
    return sb.toString();
  }

  private static void addStackString(StackTraceElement[] stackElems, StringBuilder sb) {
    sb.append(System.lineSeparator());
    for (StackTraceElement stackElem : stackElems) {
      sb.append(stackElem).append(System.lineSeparator());
    }
  }

  /**
   * Finds a free port on the machine.
   *
   * @return
   * @throws IOException
   */
  public static int findFreePort() throws IOException {
    ServerSocket socket= new ServerSocket(0);
    int port = socket.getLocalPort();
    socket.close();
    return port;
  }

  /**
   * Finds a free port on the machine, but allow the
   * ability to specify a port number to not use, no matter what.
   */
  public static int findFreePortExcepting(int portToExclude) throws IOException {
    ServerSocket socket1 = null;
    ServerSocket socket2 = null;
    try {
      socket1 = new ServerSocket(0);
      socket2 = new ServerSocket(0);
      if (socket1.getLocalPort() != portToExclude) {
        return socket1.getLocalPort();
      }
      // If we're here, then socket1.getLocalPort was the port to exclude
      // Since both sockets were open together at a point in time, we're
      // guaranteed that socket2.getLocalPort() is not the same.
      return socket2.getLocalPort();
    } finally {
      if (socket1 != null){
        socket1.close();
      }
      if (socket2 != null){
        socket2.close();
      }
    }
  }

  /**
   * Catches exceptions that can't be handled and bundles them to MetaException
   *
   * @param e
   * @throws MetaException
   */
  static void logAndThrowMetaException(Exception e) throws MetaException {
    String exInfo = "Got exception: " + e.getClass().getName() + " "
        + e.getMessage();
    LOG.error(exInfo, e);
    LOG.error("Converting exception to MetaException");
    throw new MetaException(exInfo);
  }

  /**
   * @param tableName
   * @param deserializer
   * @return the list of fields
   * @throws SerDeException
   * @throws MetaException
   */
  public static List<FieldSchema> getFieldsFromDeserializer(String tableName,
      Deserializer deserializer) throws SerDeException, MetaException {
    ObjectInspector oi = deserializer.getObjectInspector();
    String[] names = tableName.split("\\.");
    String last_name = names[names.length - 1];
    for (int i = 1; i < names.length; i++) {

      if (oi instanceof StructObjectInspector) {
        StructObjectInspector soi = (StructObjectInspector) oi;
        StructField sf = soi.getStructFieldRef(names[i]);
        if (sf == null) {
          throw new MetaException("Invalid Field " + names[i]);
        } else {
          oi = sf.getFieldObjectInspector();
        }
      } else if (oi instanceof ListObjectInspector
          && names[i].equalsIgnoreCase("$elem$")) {
        ListObjectInspector loi = (ListObjectInspector) oi;
        oi = loi.getListElementObjectInspector();
      } else if (oi instanceof MapObjectInspector
          && names[i].equalsIgnoreCase("$key$")) {
        MapObjectInspector moi = (MapObjectInspector) oi;
        oi = moi.getMapKeyObjectInspector();
      } else if (oi instanceof MapObjectInspector
          && names[i].equalsIgnoreCase("$value$")) {
        MapObjectInspector moi = (MapObjectInspector) oi;
        oi = moi.getMapValueObjectInspector();
      } else {
        throw new MetaException("Unknown type for " + names[i]);
      }
    }

    ArrayList<FieldSchema> str_fields = new ArrayList<FieldSchema>();
    // rules on how to recurse the ObjectInspector based on its type
    if (oi.getCategory() != Category.STRUCT) {
      str_fields.add(new FieldSchema(last_name, oi.getTypeName(),
          FROM_SERIALIZER));
    } else {
      List<? extends StructField> fields = ((StructObjectInspector) oi)
          .getAllStructFieldRefs();
      for (int i = 0; i < fields.size(); i++) {
        StructField structField = fields.get(i);
        String fieldName = structField.getFieldName();
        String fieldTypeName = structField.getFieldObjectInspector().getTypeName();
        String fieldComment = determineFieldComment(structField.getFieldComment());

        str_fields.add(new FieldSchema(fieldName, fieldTypeName, fieldComment));
      }
    }
    return str_fields;
  }

  private static final String FROM_SERIALIZER = "from deserializer";
  private static String determineFieldComment(String comment) {
    return (comment == null) ? FROM_SERIALIZER : comment;
  }

  /**
   * Convert TypeInfo to FieldSchema.
   */
  public static FieldSchema getFieldSchemaFromTypeInfo(String fieldName,
      TypeInfo typeInfo) {
    return new FieldSchema(fieldName, typeInfo.getTypeName(),
        "generated by TypeInfoUtils.getFieldSchemaFromTypeInfo");
  }

  /**
   * Determines whether a table is an external table.
   *
   * @param table table of interest
   *
   * @return true if external
   */
  public static boolean isExternalTable(Table table) {
    if (table == null) {
      return false;
    }
    Map<String, String> params = table.getParameters();
    if (params == null) {
      return false;
    }

    return "TRUE".equalsIgnoreCase(params.get("EXTERNAL"));
  }

  public static boolean isArchived(
      org.apache.hadoop.hive.metastore.api.Partition part) {
    Map<String, String> params = part.getParameters();
    return "TRUE".equalsIgnoreCase(params.get(hive_metastoreConstants.IS_ARCHIVED));
  }

  /**
   * Filter that filters out hidden files
   */
  private static final PathFilter hiddenFileFilter = new PathFilter() {
    @Override
    public boolean accept(Path p) {
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  /**
   * Utility method that determines if a specified directory already has
   * contents (non-hidden files) or not - useful to determine if an
   * immutable table already has contents, for example.
   *
   * @param path
   * @throws IOException
   */
  public static boolean isDirEmpty(FileSystem fs, Path path) throws IOException {

    if (fs.exists(path)) {
      FileStatus[] status = fs.globStatus(new Path(path, "*"), hiddenFileFilter);
      if (status.length > 0) {
        return false;
      }
    }
    return true;
  }

  public static String getIndexTableName(String dbName, String baseTblName, String indexName) {
    return dbName + "__" + baseTblName + "_" + indexName + "__";
  }

  public static boolean isIndexTable(Table table) {
    if (table == null) {
      return false;
    }
    return TableType.INDEX_TABLE.toString().equals(table.getTableType());
  }

  public static boolean isMaterializedViewTable(Table table) {
    if (table == null) {
      return false;
    }
    return TableType.MATERIALIZED_VIEW.toString().equals(table.getTableType());
  }

  public static boolean isView(Table table) {
    if (table == null) {
      return false;
    }
    return TableType.VIRTUAL_VIEW.toString().equals(table.getTableType());
  }

  @SuppressWarnings("unchecked")
  public static Class<? extends RawStore> getClass(String rawStoreClassName)
      throws MetaException {
    try {
      return (Class<? extends RawStore>)
          Class.forName(rawStoreClassName, true, JavaUtils.getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new MetaException(rawStoreClassName + " class not found");
    }
  }

  /**
   * Create an object of the given class.
   * @param theClass
   * @param parameterTypes
   *          an array of parameterTypes for the constructor
   * @param initargs
   *          the list of arguments for the constructor
   */
  public static <T> T newInstance(Class<T> theClass, Class<?>[] parameterTypes,
      Object[] initargs) {
    // Perform some sanity checks on the arguments.
    if (parameterTypes.length != initargs.length) {
      throw new IllegalArgumentException(
          "Number of constructor parameter types doesn't match number of arguments");
    }
    for (int i = 0; i < parameterTypes.length; i++) {
      Class<?> clazz = parameterTypes[i];
      if (initargs[i] != null && !(clazz.isInstance(initargs[i]))) {
        throw new IllegalArgumentException("Object : " + initargs[i]
            + " is not an instance of " + clazz);
      }
    }

    try {
      Constructor<T> meth = theClass.getDeclaredConstructor(parameterTypes);
      meth.setAccessible(true);
      return meth.newInstance(initargs);
    } catch (Exception e) {
      throw new RuntimeException("Unable to instantiate " + theClass.getName(), e);
    }
  }

  /**
   * @param schema1: The first schema to be compared
   * @param schema2: The second schema to be compared
   * @return true if the two schemas are the same else false
   *         for comparing a field we ignore the comment it has
   */
  public static boolean compareFieldColumns(List<FieldSchema> schema1, List<FieldSchema> schema2) {
    if (schema1.size() != schema2.size()) {
      return false;
    }
    Iterator<FieldSchema> its1 = schema1.iterator();
    Iterator<FieldSchema> its2 = schema2.iterator();
    while (its1.hasNext()) {
      FieldSchema f1 = its1.next();
      FieldSchema f2 = its2.next();
      // The default equals provided by thrift compares the comments too for
      // equality, thus we need to compare the relevant fields here.
      if (!StringUtils.equals(f1.getName(), f2.getName()) ||
          !StringUtils.equals(f1.getType(), f2.getType())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Read and return the meta store Sasl configuration. Currently it uses the default
   * Hadoop SASL configuration and can be configured using "hadoop.rpc.protection"
   * HADOOP-10211, made a backward incompatible change due to which this call doesn't
   * work with Hadoop 2.4.0 and later.
   * @param conf
   * @return The SASL configuration
   */
  public static Map<String, String> getMetaStoreSaslProperties(HiveConf conf, boolean useSSL) {
    // As of now Hive Meta Store uses the same configuration as Hadoop SASL configuration

    // If SSL is enabled, override the given value of "hadoop.rpc.protection" and set it to "authentication"
    // This disables any encryption provided by SASL, since SSL already provides it
    String hadoopRpcProtectionVal = conf.get(CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION);
    String hadoopRpcProtectionAuth = SaslRpcServer.QualityOfProtection.AUTHENTICATION.toString();

    if (useSSL && hadoopRpcProtectionVal != null && !hadoopRpcProtectionVal.equals(hadoopRpcProtectionAuth)) {
      LOG.warn("Overriding value of " + CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION + " setting it from "
              + hadoopRpcProtectionVal + " to " + hadoopRpcProtectionAuth + " because SSL is enabled");
      conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION, hadoopRpcProtectionAuth);
    }
    return HadoopThriftAuthBridge.getBridge().getHadoopSaslProperties(conf);
  }


  public static String ARCHIVING_LEVEL = "archiving_level";
  public static int getArchivingLevel(Partition part) throws MetaException {
    if (!isArchived(part)) {
      throw new MetaException("Getting level of unarchived partition");
    }

    String lv = part.getParameters().get(ARCHIVING_LEVEL);
    if (lv != null) {
      return Integer.parseInt(lv);
    }
     // partitions archived before introducing multiple archiving
    return part.getValues().size();
  }

  public static String[] getQualifiedName(String defaultDbName, String tableName) {
    String[] names = tableName.split("\\.");
    if (names.length == 1) {
      return new String[] { defaultDbName, tableName};
    }
    return names;
  }

  public static List<String> getColumnNames(List<FieldSchema> schema) {
    List<String> cols = new ArrayList<>(schema.size());
    for (FieldSchema fs : schema) {
      cols.add(fs.getName());
    }
    return cols;
  }
}
