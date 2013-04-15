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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.hadoop.util.StringUtils;

public class MetaStoreUtils {

  protected static final Log LOG = LogFactory.getLog("hive.log");

  public static final String DEFAULT_DATABASE_NAME = "default";
  public static final String DEFAULT_DATABASE_COMMENT = "Default Hive database";

  public static final String DATABASE_WAREHOUSE_SUFFIX = ".db";

  /**
   * printStackTrace
   *
   * Helper function to print an exception stack trace to the log and not stderr
   *
   * @param e
   *          the exception
   *
   */
  static public void printStackTrace(Exception e) {
    for (StackTraceElement s : e.getStackTrace()) {
      LOG.error(s);
    }
  }

  public static Table createColumnsetSchema(String name, List<String> columns,
      List<String> partCols, Configuration conf) throws MetaException {

    if (columns == null) {
      throw new MetaException("columns not specified for table " + name);
    }

    Table tTable = new Table();
    tTable.setTableName(name);
    tTable.setSd(new StorageDescriptor());
    StorageDescriptor sd = tTable.getSd();
    sd.setSerdeInfo(new SerDeInfo());
    SerDeInfo serdeInfo = sd.getSerdeInfo();
    serdeInfo.setSerializationLib(LazySimpleSerDe.class.getName());
    serdeInfo.setParameters(new HashMap<String, String>());
    serdeInfo.getParameters().put(
        org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT, "1");

    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    sd.setCols(fields);
    for (String col : columns) {
      FieldSchema field = new FieldSchema(col,
          org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME, "'default'");
      fields.add(field);
    }

    tTable.setPartitionKeys(new ArrayList<FieldSchema>());
    for (String partCol : partCols) {
      FieldSchema part = new FieldSchema();
      part.setName(partCol);
      part.setType(org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME); // default
                                                                             // partition
                                                                             // key
      tTable.getPartitionKeys().add(part);
    }
    sd.setNumBuckets(-1);
    return tTable;
  }

  /**
   * recursiveDelete
   *
   * just recursively deletes a dir - you'd think Java would have something to
   * do this??
   *
   * @param f
   *          - the file/dir to delete
   * @exception IOException
   *              propogate f.delete() exceptions
   *
   */
  static public void recursiveDelete(File f) throws IOException {
    if (f.isDirectory()) {
      File fs[] = f.listFiles();
      for (File subf : fs) {
        recursiveDelete(subf);
      }
    }
    if (!f.delete()) {
      throw new IOException("could not delete: " + f.getPath());
    }
  }

  /**
   * getDeserializer
   *
   * Get the Deserializer for a table given its name and properties.
   *
   * @param conf
   *          hadoop config
   * @param schema
   *          the properties to use to instantiate the deserializer
   * @return
   *   Returns instantiated deserializer by looking up class name of deserializer stored in passed
   *   in properties. Also, initializes the deserializer with schema stored in passed in properties.
   * @exception MetaException
   *              if any problems instantiating the Deserializer
   *
   *              todo - this should move somewhere into serde.jar
   *
   */
  static public Deserializer getDeserializer(Configuration conf,
      Properties schema) throws MetaException {
    String lib = schema
        .getProperty(org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB);
    try {
      Deserializer deserializer = SerDeUtils.lookupDeserializer(lib);
      (deserializer).initialize(conf, schema);
      return deserializer;
    } catch (Exception e) {
      LOG.error("error in initSerDe: " + e.getClass().getName() + " "
          + e.getMessage());
      MetaStoreUtils.printStackTrace(e);
      throw new MetaException(e.getClass().getName() + " " + e.getMessage());
    }
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
      org.apache.hadoop.hive.metastore.api.Table table) throws MetaException {
    String lib = table.getSd().getSerdeInfo().getSerializationLib();
    if (lib == null) {
      return null;
    }
    try {
      Deserializer deserializer = SerDeUtils.lookupDeserializer(lib);
      deserializer.initialize(conf, MetaStoreUtils.getTableMetadata(table));
      return deserializer;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      LOG.error("error in initSerDe: " + e.getClass().getName() + " "
          + e.getMessage());
      MetaStoreUtils.printStackTrace(e);
      throw new MetaException(e.getClass().getName() + " " + e.getMessage());
    }
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
      Deserializer deserializer = SerDeUtils.lookupDeserializer(lib);
      deserializer.initialize(conf, MetaStoreUtils.getPartitionMetadata(part, table));
      return deserializer;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      LOG.error("error in initSerDe: " + e.getClass().getName() + " "
          + e.getMessage());
      MetaStoreUtils.printStackTrace(e);
      throw new MetaException(e.getClass().getName() + " " + e.getMessage());
    }
  }

  static public void deleteWHDirectory(Path path, Configuration conf,
      boolean use_trash) throws MetaException {

    try {
      if (!path.getFileSystem(conf).exists(path)) {
        LOG.warn("drop data called on table/partition with no directory: "
            + path);
        return;
      }

      if (use_trash) {

        int count = 0;
        Path newPath = new Path("/Trash/Current"
            + path.getParent().toUri().getPath());

        if (path.getFileSystem(conf).exists(newPath) == false) {
          path.getFileSystem(conf).mkdirs(newPath);
        }

        do {
          newPath = new Path("/Trash/Current" + path.toUri().getPath() + "."
              + count);
          if (path.getFileSystem(conf).exists(newPath)) {
            count++;
            continue;
          }
          if (path.getFileSystem(conf).rename(path, newPath)) {
            break;
          }
        } while (++count < 50);
        if (count >= 50) {
          throw new MetaException("Rename failed due to maxing out retries");
        }
      } else {
        // directly delete it
        path.getFileSystem(conf).delete(path, true);
      }
    } catch (IOException e) {
      LOG.error("Got exception trying to delete data dir: " + e);
      throw new MetaException(e.getMessage());
    } catch (MetaException e) {
      LOG.error("Got exception trying to delete data dir: " + e);
      throw e;
    }
  }

  /**
   * validateName
   *
   * Checks the name conforms to our standars which are: "[a-zA-z_0-9]+". checks
   * this is just characters and numbers and _
   *
   * @param name
   *          the name to validate
   * @return true or false depending on conformance
   * @exception MetaException
   *              if it doesn't match the pattern.
   */
  static public boolean validateName(String name) {
    Pattern tpat = Pattern.compile("[\\w_]+");
    Matcher m = tpat.matcher(name);
    if (m.matches()) {
      return true;
    }
    return false;
  }

  static public boolean validateTblColumns(List<FieldSchema> cols) {
    for (FieldSchema fieldSchema : cols) {
      if (!validateName(fieldSchema.getName())) {
        return false;
      }
      if (!validateColumnType(fieldSchema.getType())) {
        return false;
      }
    }
    return true;
  }

  /**
   * validate column type
   *
   * if it is predefined, yes. otherwise no
   * @param name
   * @return
   */
  static public boolean validateColumnType(String type) {
    int last = 0;
    boolean lastAlphaDigit = Character.isLetterOrDigit(type.charAt(last));
    for (int i = 1; i <= type.length(); i++) {
      if (i == type.length()
          || Character.isLetterOrDigit(type.charAt(i)) != lastAlphaDigit) {
        String token = type.substring(last, i);
        last = i;
        if (!hiveThriftTypeMap.contains(token)) {
          return false;
        }
        break;
      }
    }
    return true;
  }

  public static  boolean validateSkewedColNames(List<String> cols) {
    if (null == cols) {
      return true;
    }
    for (String col : cols) {
      if (!validateName(col)) {
        return false;
      }
    }
    return true;
  }

  public static boolean validateSkewedColNamesSubsetCol(List<String> skewedColNames,
      List<FieldSchema> cols) {
    if (null == skewedColNames) {
      return true;
    }
    List<String> colNames = new ArrayList<String>();
    for (FieldSchema fieldSchema : cols) {
      colNames.add(fieldSchema.getName());
    }
    // make a copy
    List<String> copySkewedColNames = new ArrayList<String>(skewedColNames);
    // remove valid columns
    copySkewedColNames.removeAll(colNames);
    return (copySkewedColNames.size() > 0) ? false : true;
  }

  public static String getListType(String t) {
    return "array<" + t + ">";
  }

  public static String getMapType(String k, String v) {
    return "map<" + k + "," + v + ">";
  }

  public static void setSerdeParam(SerDeInfo sdi, Properties schema,
      String param) {
    String val = schema.getProperty(param);
    if (org.apache.commons.lang.StringUtils.isNotBlank(val)) {
      sdi.getParameters().put(param, val);
    }
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
  }

  static Set<String> hiveThriftTypeMap; //for validation
  static {
    hiveThriftTypeMap = new HashSet<String>();
    hiveThriftTypeMap.addAll(org.apache.hadoop.hive.serde.serdeConstants.PrimitiveTypes);
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
   * Convert FieldSchemas to Thrift DDL + column names and column types
   *
   * @param structName
   *          The name of the table
   * @param fieldSchemas
   *          List of fields along with their schemas
   * @return String containing "Thrift
   *         DDL#comma-separated-column-names#colon-separated-columntypes
   *         Example:
   *         "struct result { a string, map<int,string> b}#a,b#string:map<int,string>"
   */
  public static String getFullDDLFromFieldSchema(String structName,
      List<FieldSchema> fieldSchemas) {
    StringBuilder ddl = new StringBuilder();
    ddl.append(getDDLFromFieldSchema(structName, fieldSchemas));
    ddl.append('#');
    StringBuilder colnames = new StringBuilder();
    StringBuilder coltypes = new StringBuilder();
    boolean first = true;
    for (FieldSchema col : fieldSchemas) {
      if (first) {
        first = false;
      } else {
        colnames.append(',');
        coltypes.append(':');
      }
      colnames.append(col.getName());
      coltypes.append(col.getType());
    }
    ddl.append(colnames);
    ddl.append('#');
    ddl.append(coltypes);
    return ddl.toString();
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

    LOG.debug("DDL: " + ddl);
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
        schema.put(key, (param.getValue() != null) ? param.getValue() : "");
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

  public static Properties getSchema(
      org.apache.hadoop.hive.metastore.api.StorageDescriptor sd,
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
        schema.put(param.getKey(), (param.getValue() != null) ? param.getValue() : "");
      }

      if (sd.getSerdeInfo().getSerializationLib() != null) {
        schema.setProperty(
            org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB, sd
                .getSerdeInfo().getSerializationLib());
      }
    }
    StringBuilder colNameBuf = new StringBuilder();
    StringBuilder colTypeBuf = new StringBuilder();
    boolean first = true;
    for (FieldSchema col : tblsd.getCols()) {
      if (!first) {
        colNameBuf.append(",");
        colTypeBuf.append(":");
      }
      colNameBuf.append(col.getName());
      colTypeBuf.append(col.getType());
      first = false;
    }
    String colNames = colNameBuf.toString();
    String colTypes = colTypeBuf.toString();
    schema.setProperty(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS,
        colNames);
    schema.setProperty(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES,
        colTypes);
    if (sd.getCols() != null) {
      schema.setProperty(
          org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_DDL,
          getDDLFromFieldSchema(tableName, sd.getCols()));
    }

    String partString = "";
    String partStringSep = "";
    for (FieldSchema partKey : partitionKeys) {
      partString = partString.concat(partStringSep);
      partString = partString.concat(partKey.getName());
      if (partStringSep.length() == 0) {
        partStringSep = "/";
      }
    }
    if (partString.length() > 0) {
      schema
          .setProperty(
              org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS,
              partString);
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

  /**
   * Convert FieldSchemas to columnNames.
   */
  public static String getColumnNamesFromFieldSchema(
      List<FieldSchema> fieldSchemas) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < fieldSchemas.size(); i++) {
      if (i > 0) {
        sb.append(",");
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

  public static void makeDir(Path path, HiveConf hiveConf) throws MetaException {
    FileSystem fs;
    try {
      fs = path.getFileSystem(hiveConf);
      if (!fs.exists(path)) {
        fs.mkdirs(path);
      }
    } catch (IOException e) {
      throw new MetaException("Unable to : " + path);
    }

  }

  public static void startMetaStore(final int port,
      final HadoopThriftAuthBridge bridge) throws Exception {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          HiveMetaStore.startMetaStore(port, bridge);
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
        if (retries++ > 6) { //give up
          exc = e;
          break;
        }
        Thread.sleep(10000);
      }
    }
    throw exc;
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
   * Catches exceptions that can't be handled and bundles them to MetaException
   *
   * @param e
   * @throws MetaException
   */
  static void logAndThrowMetaException(Exception e) throws MetaException {
    LOG
        .error("Got exception: " + e.getClass().getName() + " "
            + e.getMessage());
    LOG.error(StringUtils.stringifyException(e));
    throw new MetaException("Got exception: " + e.getClass().getName() + " "
        + e.getMessage());
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
    return (comment == null || comment.isEmpty()) ? FROM_SERIALIZER : comment;
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
    if ("true".equalsIgnoreCase(params.get(hive_metastoreConstants.IS_ARCHIVED))) {
      return true;
    } else {
      return false;
    }
  }

  public static Path getOriginalLocation(
      org.apache.hadoop.hive.metastore.api.Partition part) {
    Map<String, String> params = part.getParameters();
    assert(isArchived(part));
    String originalLocation = params.get(hive_metastoreConstants.ORIGINAL_LOCATION);
    assert( originalLocation != null);

    return new Path(originalLocation);
  }

  public static boolean isNonNativeTable(Table table) {
    if (table == null) {
      return false;
    }
    return (table.getParameters().get(hive_metastoreConstants.META_TABLE_STORAGE) != null);
  }

  /**
   * Returns true if partial has the same values as full for all values that
   * aren't empty in partial.
   */

  public static boolean pvalMatches(List<String> partial, List<String> full) {
    if(partial.size() > full.size()) {
      return false;
    }
    Iterator<String> p = partial.iterator();
    Iterator<String> f = full.iterator();

    while(p.hasNext()) {
      String pval = p.next();
      String fval = f.next();

      if (pval.length() != 0 && !pval.equals(fval)) {
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

  /**
   * Given a map of partition column names to values, this creates a filter
   * string that can be used to call the *byFilter methods
   * @param m
   * @return the filter string
   */
  public static String makeFilterStringFromMap(Map<String, String> m) {
    StringBuilder filter = new StringBuilder();
    for (Entry<String, String> e : m.entrySet()) {
      String col = e.getKey();
      String val = e.getValue();
      if (filter.length() == 0) {
        filter.append(col + "=\"" + val + "\"");
      } else {
        filter.append(" and " + col + "=\"" + val + "\"");
      }
    }
    return filter.toString();
  }

  /**
   * create listener instances as per the configuration.
   *
   * @param clazz
   * @param conf
   * @param listenerImplList
   * @return
   * @throws MetaException
   */
  static <T> List<T> getMetaStoreListeners(Class<T> clazz,
      HiveConf conf, String listenerImplList) throws MetaException {

    List<T> listeners = new ArrayList<T>();
    listenerImplList = listenerImplList.trim();
    if (listenerImplList.equals("")) {
      return listeners;
    }

    String[] listenerImpls = listenerImplList.split(",");
    for (String listenerImpl : listenerImpls) {
      try {
        T listener = (T) Class.forName(
            listenerImpl.trim(), true, JavaUtils.getClassLoader()).getConstructor(
                Configuration.class).newInstance(conf);
        listeners.add(listener);
      } catch (InvocationTargetException ie) {
        throw new MetaException("Failed to instantiate listener named: "+
            listenerImpl + ", reason: " + ie.getCause());
      } catch (Exception e) {
        throw new MetaException("Failed to instantiate listener named: "+
            listenerImpl + ", reason: " + e);
      }
    }

    return listeners;
  }

  public static Class<?> getClass(String rawStoreClassName)
      throws MetaException {
    try {
      return Class.forName(rawStoreClassName, true, JavaUtils.getClassLoader());
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
      if (!(clazz.isInstance(initargs[i]))) {
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

  public static void validatePartitionNameCharacters(List<String> partVals,
      Pattern partitionValidationPattern) throws MetaException {

    String invalidPartitionVal =
        getPartitionValWithInvalidCharacter(partVals, partitionValidationPattern);
    if (invalidPartitionVal != null) {
      throw new MetaException("Partition value '" + invalidPartitionVal +
          "' contains a character " + "not matched by whitelist pattern '" +
          partitionValidationPattern.toString() + "'.  " + "(configure with " +
          HiveConf.ConfVars.METASTORE_PARTITION_NAME_WHITELIST_PATTERN.varname + ")");
      }
  }

  public static boolean partitionNameHasValidCharacters(List<String> partVals,
      Pattern partitionValidationPattern) {
    return getPartitionValWithInvalidCharacter(partVals, partitionValidationPattern) == null;
  }

  private static String getPartitionValWithInvalidCharacter(List<String> partVals,
      Pattern partitionValidationPattern) {
    if (partitionValidationPattern == null) {
      return null;
    }

    for (String partVal : partVals) {
      if (!partitionValidationPattern.matcher(partVal).matches()) {
        return partVal;
      }
    }

    return null;
  }

}
