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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Constants;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.util.StringUtils;

public class MetaStoreUtils {

  protected static final Log LOG = LogFactory.getLog("hive.log");

  public static final String DEFAULT_DATABASE_NAME = "default";

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
        org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, "1");

    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    sd.setCols(fields);
    for (String col : columns) {
      FieldSchema field = new FieldSchema(col,
          org.apache.hadoop.hive.serde.Constants.STRING_TYPE_NAME, "'default'");
      fields.add(field);
    }

    tTable.setPartitionKeys(new ArrayList<FieldSchema>());
    for (String partCol : partCols) {
      FieldSchema part = new FieldSchema();
      part.setName(partCol);
      part.setType(org.apache.hadoop.hive.serde.Constants.STRING_TYPE_NAME); // default
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
   * @return the Deserializer
   * @exception MetaException
   *              if any problems instantiating the Deserializer
   * 
   *              todo - this should move somewhere into serde.jar
   * 
   */
  static public Deserializer getDeserializer(Configuration conf,
      Properties schema) throws MetaException {
    String lib = schema
        .getProperty(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_LIB);
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
   * @return the Deserializer
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
      deserializer.initialize(conf, MetaStoreUtils.getSchema(table));
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
   * @param partition
   *          the partition
   * @return the Deserializer
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
      deserializer.initialize(conf, MetaStoreUtils.getSchema(part, table));
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

  static public boolean validateColNames(List<FieldSchema> cols) {
    for (FieldSchema fieldSchema : cols) {
      if (!validateName(fieldSchema.getName())) {
        return false;
      }
    }
    return true;
  }

  public static String getListType(String t) {
    return "array<" + t + ">";
  }

  public static String getMapType(String k, String v) {
    return "map<" + k + "," + v + ">";
  }

  public static Table getTable(Configuration conf, Properties schema)
      throws MetaException {
    Table t = new Table();
    t.setSd(new StorageDescriptor());
    t
        .setTableName(schema
            .getProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME));
    t
        .getSd()
        .setLocation(
            schema
                .getProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_LOCATION));
    t.getSd().setInputFormat(
        schema.getProperty(
            org.apache.hadoop.hive.metastore.api.Constants.FILE_INPUT_FORMAT,
            org.apache.hadoop.mapred.SequenceFileInputFormat.class.getName()));
    t.getSd().setOutputFormat(
        schema.getProperty(
            org.apache.hadoop.hive.metastore.api.Constants.FILE_OUTPUT_FORMAT,
            org.apache.hadoop.mapred.SequenceFileOutputFormat.class.getName()));
    t.setPartitionKeys(new ArrayList<FieldSchema>());
    t.setDbName(MetaStoreUtils.DEFAULT_DATABASE_NAME);
    String part_cols_str = schema
        .getProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_PARTITION_COLUMNS);
    t.setPartitionKeys(new ArrayList<FieldSchema>());
    if (part_cols_str != null && (part_cols_str.trim().length() != 0)) {
      String[] part_keys = part_cols_str.trim().split("/");
      for (String key : part_keys) {
        FieldSchema part = new FieldSchema();
        part.setName(key);
        part.setType(org.apache.hadoop.hive.serde.Constants.STRING_TYPE_NAME); // default
                                                                               // partition
                                                                               // key
        t.getPartitionKeys().add(part);
      }
    }
    t.getSd()
        .setNumBuckets(
            Integer.parseInt(schema.getProperty(
                org.apache.hadoop.hive.metastore.api.Constants.BUCKET_COUNT,
                "-1")));
    String bucketFieldName = schema
        .getProperty(org.apache.hadoop.hive.metastore.api.Constants.BUCKET_FIELD_NAME);
    t.getSd().setBucketCols(new ArrayList<String>(1));
    if ((bucketFieldName != null) && (bucketFieldName.trim().length() != 0)) {
      t.getSd().setBucketCols(new ArrayList<String>(1));
      t.getSd().getBucketCols().add(bucketFieldName);
    }

    t.getSd().setSerdeInfo(new SerDeInfo());
    t.getSd().getSerdeInfo().setParameters(new HashMap<String, String>());
    t.getSd().getSerdeInfo().setName(t.getTableName());
    t
        .getSd()
        .getSerdeInfo()
        .setSerializationLib(
            schema
                .getProperty(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_LIB));
    setSerdeParam(t.getSd().getSerdeInfo(), schema,
        org.apache.hadoop.hive.serde.Constants.SERIALIZATION_CLASS);
    setSerdeParam(t.getSd().getSerdeInfo(), schema,
        org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT);
    if (org.apache.commons.lang.StringUtils
        .isNotBlank(schema
            .getProperty(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_CLASS))) {
      setSerdeParam(t.getSd().getSerdeInfo(), schema,
          org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_SERDE);
    }
    // needed for MetadataTypedColumnSetSerDe and LazySimpleSerDe
    setSerdeParam(t.getSd().getSerdeInfo(), schema,
        org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_COLUMNS);
    // needed for LazySimpleSerDe
    setSerdeParam(t.getSd().getSerdeInfo(), schema,
        org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_COLUMN_TYPES);
    // needed for DynamicSerDe
    setSerdeParam(t.getSd().getSerdeInfo(), schema,
        org.apache.hadoop.hive.serde.Constants.SERIALIZATION_DDL);

    String colstr = schema
        .getProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_COLUMNS);
    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    if (colstr != null) {
      String[] cols = colstr.split(",");
      for (String colName : cols) {
        FieldSchema col = new FieldSchema(colName,
            org.apache.hadoop.hive.serde.Constants.STRING_TYPE_NAME,
            "'default'");
        fields.add(col);
      }
    }

    if (fields.size() == 0) {
      // get the fields from serde
      try {
        fields = getFieldsFromDeserializer(t.getTableName(), getDeserializer(
            conf, schema));
      } catch (SerDeException e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new MetaException("Invalid serde or schema. " + e.getMessage());
      }
    }
    t.getSd().setCols(fields);

    t.setOwner(schema.getProperty("creator"));

    // remove all the used up parameters to find out the remaining parameters
    schema.remove(Constants.META_TABLE_NAME);
    schema.remove(Constants.META_TABLE_LOCATION);
    schema.remove(Constants.FILE_INPUT_FORMAT);
    schema.remove(Constants.FILE_OUTPUT_FORMAT);
    schema.remove(Constants.META_TABLE_PARTITION_COLUMNS);
    schema.remove(Constants.BUCKET_COUNT);
    schema.remove(Constants.BUCKET_FIELD_NAME);
    schema.remove(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_CLASS);
    schema.remove(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT);
    schema.remove(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_LIB);
    schema.remove(Constants.META_TABLE_SERDE);
    schema.remove(Constants.META_TABLE_COLUMNS);
    schema.remove(Constants.META_TABLE_COLUMN_TYPES);

    // add the remaining unknown parameters to the table's parameters
    t.setParameters(new HashMap<String, String>());
    for (Entry<Object, Object> e : schema.entrySet()) {
      t.getParameters().put(e.getKey().toString(), e.getValue().toString());
    }

    return t;
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
        org.apache.hadoop.hive.serde.Constants.BOOLEAN_TYPE_NAME, "bool");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.Constants.TINYINT_TYPE_NAME, "byte");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.Constants.SMALLINT_TYPE_NAME, "i16");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.Constants.INT_TYPE_NAME, "i32");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.Constants.BIGINT_TYPE_NAME, "i64");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.Constants.DOUBLE_TYPE_NAME, "double");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.Constants.FLOAT_TYPE_NAME, "float");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.Constants.LIST_TYPE_NAME, "list");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.Constants.MAP_TYPE_NAME, "map");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.Constants.STRING_TYPE_NAME, "string");
    // These 3 types are not supported yet.
    // We should define a complex type date in thrift that contains a single int
    // member, and DynamicSerDe
    // should convert it to date type at runtime.
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.Constants.DATE_TYPE_NAME, "date");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.Constants.DATETIME_TYPE_NAME, "datetime");
    typeToThriftTypeMap
        .put(org.apache.hadoop.hive.serde.Constants.TIMESTAMP_TYPE_NAME,
            "timestamp");
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

    LOG.info("DDL: " + ddl);
    return ddl.toString();
  }

  public static Properties getSchema(
      org.apache.hadoop.hive.metastore.api.Table table) {
    return MetaStoreUtils.getSchema(table.getSd(), table.getSd(), table
        .getParameters(), table.getTableName(), table.getPartitionKeys());
  }

  public static Properties getSchema(
      org.apache.hadoop.hive.metastore.api.Partition part,
      org.apache.hadoop.hive.metastore.api.Table table) {
    return MetaStoreUtils.getSchema(part.getSd(), table.getSd(), table
        .getParameters(), table.getTableName(), table.getPartitionKeys());
  }

  public static Properties getSchema(
      org.apache.hadoop.hive.metastore.api.StorageDescriptor sd,
      org.apache.hadoop.hive.metastore.api.StorageDescriptor tblsd,
      Map<String, String> parameters, String tableName,
      List<FieldSchema> partitionKeys) {
    Properties schema = new Properties();
    String inputFormat = sd.getInputFormat();
    if (inputFormat == null || inputFormat.length() == 0) {
      inputFormat = org.apache.hadoop.mapred.SequenceFileInputFormat.class
          .getName();
    }
    schema.setProperty(
        org.apache.hadoop.hive.metastore.api.Constants.FILE_INPUT_FORMAT,
        inputFormat);
    String outputFormat = sd.getOutputFormat();
    if (outputFormat == null || outputFormat.length() == 0) {
      outputFormat = org.apache.hadoop.mapred.SequenceFileOutputFormat.class
          .getName();
    }
    schema.setProperty(
        org.apache.hadoop.hive.metastore.api.Constants.FILE_OUTPUT_FORMAT,
        outputFormat);
    schema.setProperty(
        org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME,
        tableName);
    if (sd.getLocation() != null) {
      schema.setProperty(
          org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_LOCATION,
          sd.getLocation());
    }
    schema.setProperty(
        org.apache.hadoop.hive.metastore.api.Constants.BUCKET_COUNT, Integer
            .toString(sd.getNumBuckets()));
    if (sd.getBucketCols() != null && sd.getBucketCols().size() > 0) {
      schema.setProperty(
          org.apache.hadoop.hive.metastore.api.Constants.BUCKET_FIELD_NAME, sd
              .getBucketCols().get(0));
    }
    if (sd.getSerdeInfo() != null) {
      schema.putAll(sd.getSerdeInfo().getParameters());
      if (sd.getSerdeInfo().getSerializationLib() != null) {
        schema.setProperty(
            org.apache.hadoop.hive.serde.Constants.SERIALIZATION_LIB, sd
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
        org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_COLUMNS,
        colNames);
    schema.setProperty(
        org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_COLUMN_TYPES,
        colTypes);
    if (sd.getCols() != null) {
      schema.setProperty(
          org.apache.hadoop.hive.serde.Constants.SERIALIZATION_DDL,
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
              org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_PARTITION_COLUMNS,
              partString);
    }

    if (parameters != null) {
      for (Entry<String, String> e : parameters.entrySet()) {
        schema.setProperty(e.getKey(), e.getValue());
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
          "from deserializer"));
    } else {
      List<? extends StructField> fields = ((StructObjectInspector) oi)
          .getAllStructFieldRefs();
      for (int i = 0; i < fields.size(); i++) {
        String fieldName = fields.get(i).getFieldName();
        String fieldTypeName = fields.get(i).getFieldObjectInspector()
            .getTypeName();
        str_fields.add(new FieldSchema(fieldName, fieldTypeName,
            "from deserializer"));
      }
    }
    return str_fields;
  }

  /**
   * Convert TypeInfo to FieldSchema.
   */
  public static FieldSchema getFieldSchemaFromTypeInfo(String fieldName,
      TypeInfo typeInfo) {
    return new FieldSchema(fieldName, typeInfo.getTypeName(),
        "generated by TypeInfoUtils.getFieldSchemaFromTypeInfo");
  }

}
