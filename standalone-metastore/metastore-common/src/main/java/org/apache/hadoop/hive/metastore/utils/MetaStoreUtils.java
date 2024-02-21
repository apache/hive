/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.utils;

import java.io.File;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.regex.Pattern.compile;

import javax.annotation.Nullable;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionsSpecByExprResult;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.WMPoolSchedulingPolicy;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.security.SaslRpcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

public class MetaStoreUtils {

  private static final DateTimeFormatter DATE_FORMATTER = createDateTimeFormatter("uuuu-MM-dd");

  private static final DateTimeFormatter TIMESTAMP_FORMATTER = createDateTimeFormatter("uuuu-MM-dd HH:mm:ss");

  private static DateTimeFormatter createDateTimeFormatter(String format) {
    return DateTimeFormatter.ofPattern(format).withZone(TimeZone.getTimeZone("UTC").toZoneId())
        .withResolverStyle(ResolverStyle.STRICT);
  }

  /**
   * Converts java.sql.Date to String format date.
   * @param date - java.sql.Date object.
   * @return Date in string format.
   */
  public static String convertDateToString(Date date) {
    return DATE_FORMATTER.format(date.toLocalDate());
  }

  /**
   * Converts string format date to java.sql.Date.
   * @param date Date in string format.
   * @return java.sql.Date object.
   */
  public static Date convertStringToDate(String date) {
    LocalDate val = LocalDate.parse(date, DATE_FORMATTER);
    return java.sql.Date.valueOf(val);
  }

  /**
   * Converts java.sql.Timestamp to string format timestamp.
   * @param timestamp java.sql.Timestamp object.
   * @return Timestamp in string format.
   */
  public static String convertTimestampToString(Timestamp timestamp) {
    return TIMESTAMP_FORMATTER.format(timestamp.toLocalDateTime());
  }

  /**
   * Converts timestamp string format to java.sql.Timestamp.
   * @param timestamp Timestamp in string format.
   * @return java.sql.Timestamp object.
   */
  public static Timestamp convertStringToTimestamp(String timestamp) {
    LocalDateTime val = LocalDateTime.from(TIMESTAMP_FORMATTER.parse(timestamp));
    return Timestamp.valueOf(val);
  }

  // Indicates a type was derived from the deserializer rather than Hive's metadata.
  public static final String TYPE_FROM_DESERIALIZER = "<derived from deserializer>";

  private static final Logger LOG = LoggerFactory.getLogger(MetaStoreUtils.class);

  // The following two are public for any external users who wish to use them.
  /**
   * This character is used to mark a database name as having a catalog name prepended.  This
   * marker should be placed first in the String to make it easy to determine that this has both
   * a catalog and a database name.  @ is chosen as it is not used in regular expressions.  This
   * is only intended for use when making old Thrift calls that do not support catalog names.
   */
  public static final char CATALOG_DB_THRIFT_NAME_MARKER = '@';

  /**
   * This String is used to separate the catalog name from the database name.  This should only
   * be used in Strings that are prepended with {@link #CATALOG_DB_THRIFT_NAME_MARKER}.  # is
   * chosen because it is not used in regular expressions.  this is only intended for use when
   * making old Thrift calls that do not support catalog names.
   */
  public static final String CATALOG_DB_SEPARATOR = "#";

  /**
   * Mark a database as being empty (as distinct from null).
   */
  public static final String DB_EMPTY_MARKER = "!";

  public static final String EXTERNAL_TABLE_PURGE = "external.table.purge";

  // Right now we only support one special character '/'.
  // More special characters can be added accordingly in the future.
  // NOTE:
  // If the following array is updated, please also be sure to update the
  // configuration parameter documentation
  // HIVE_SUPPORT_SPECICAL_CHARACTERS_IN_TABLE_NAMES in MetastoreConf as well.
  private static final char[] SPECIAL_CHARACTERS_IN_TABLE_NAMES = new char[] {
      // standard
      ' ', '"', '%', '&', '\'', '(', ')', '*', '+', ',', '-', '.', '/', ':', ';', '<', '=', '>', '?', '[', ']',
      '_', '|', '{', '}', '$', '^',
      // non-standard
      '!', '~', '#', '@', '`'
  };

  public static final String NO_VAL = " --- ";

  public static final String USER_NAME_HTTP_HEADER = "x-actor-username";

  /**
   * Catches exceptions that cannot be handled and wraps them in MetaException.
   *
   * @param e exception to wrap.
   * @throws MetaException wrapper for the exception
   */
  public static void throwMetaException(Exception e) throws MetaException {
    throw new MetaException("Got exception: " + e.getClass().getName() + " " + e.getMessage());
  }

  public static String encodeTableName(String name) {
    // The encoding method is simple, e.g., replace
    // all the special characters with the corresponding number in ASCII.
    // Note that unicode is not supported in table names. And we have explicit
    // checks for it.
    StringBuilder sb = new StringBuilder();
    for (char ch : name.toCharArray()) {
      if (Character.isLetterOrDigit(ch) || ch == '_') {
        sb.append(ch);
      } else {
        sb.append('-').append((int) ch).append('-');
      }
    }
    return sb.toString();
  }

  /**
   * convert Exception to MetaException, which sets the cause to such exception
   * @param e cause of the exception
   * @return  the MetaException with the specified exception as the cause
   */
  public static MetaException newMetaException(Exception e) {
    return newMetaException(e != null ? e.getMessage() : null, e);
  }

  /**
   * convert Exception to MetaException, which sets the cause to such exception
   * @param errorMessage  the error message for this MetaException
   * @param e             cause of the exception
   * @return  the MetaException with the specified exception as the cause
   */
  public static MetaException newMetaException(String errorMessage, Exception e) {
    MetaException metaException = new MetaException(errorMessage);
    if (e != null) {
      metaException.initCause(e);
    }
    return metaException;
  }


  public static List<String> getColumnNamesForTable(Table table) {
    List<String> colNames = new ArrayList<>();
    Iterator<FieldSchema> colsIterator = table.getSd().getColsIterator();
    while (colsIterator.hasNext()) {
      colNames.add(colsIterator.next().getName());
    }
    return colNames;
  }

  /*
   * Check the table storage location must not be root path.
   */
  public static boolean validateTblStorage(StorageDescriptor sd) {
    return !(StringUtils.isNotBlank(sd.getLocation())
            && new Path(sd.getLocation()).getParent() == null);
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
  public static boolean validateName(String name, Configuration conf) {
    Pattern tpat;
    StringBuilder allowedSpecialCharacters = new StringBuilder();
    if (conf != null
        && MetastoreConf.getBoolVar(conf,
        MetastoreConf.ConfVars.SUPPORT_SPECICAL_CHARACTERS_IN_TABLE_NAMES)) {
      for (Character c : SPECIAL_CHARACTERS_IN_TABLE_NAMES) {
        allowedSpecialCharacters.append(c);
      }
    }
    tpat = Pattern.compile("[\\w" + Pattern.quote(allowedSpecialCharacters.toString()) + "]+");
    Matcher m = tpat.matcher(name);
    return m.matches();
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

    return isExternal(params);
  }

  public static boolean isIcebergTable(Map<String, String> params) {
    return HiveMetaHook.ICEBERG.equalsIgnoreCase(params.get(HiveMetaHook.TABLE_TYPE));
  }

  public static boolean isTranslatedToExternalTable(Table table) {
    Map<String, String> params = table.getParameters();
    return params != null && MetaStoreUtils.isPropertyTrue(params, HiveMetaHook.EXTERNAL)
        && MetaStoreUtils.isPropertyTrue(params, HiveMetaHook.TRANSLATED_TO_EXTERNAL) && table.getSd() != null
        && table.getSd().isSetLocation();
  }

  public static String getDbNameFromReplPolicy(String replPolicy) {
    assert replPolicy != null;
    return replPolicy.split(Pattern.quote("."))[0];
  }

  public static boolean isDbReplIncompatible(Database db) {
    if (db == null) {
      return false;
    }
    Map<String, String> dbParameters = db.getParameters();
    return dbParameters != null && ReplConst.TRUE.equalsIgnoreCase(dbParameters.get(ReplConst.REPL_INCOMPATIBLE));
  }

  public static boolean isDbBeingPlannedFailedOver(Database db) {
    assert (db != null);
    Map<String, String> dbParameters = db.getParameters();
    if (dbParameters == null) {
      return false;
    }
    String dbFailoverEndPoint = dbParameters.get(ReplConst.REPL_FAILOVER_ENDPOINT);
    return FailoverEndpoint.SOURCE.toString().equalsIgnoreCase(dbFailoverEndPoint)
            || FailoverEndpoint.TARGET.toString().equalsIgnoreCase(dbFailoverEndPoint);
  }

  public static boolean isDbBeingPlannedFailedOverAtEndpoint(Database db, FailoverEndpoint endPoint) {
    if (db == null) {
      return false;
    }
    Map<String, String> dbParameters = db.getParameters();
    return dbParameters != null
            && endPoint.toString().equalsIgnoreCase(dbParameters.get(ReplConst.REPL_FAILOVER_ENDPOINT));
  }

  public static boolean isTargetOfReplication(Database db) {
    assert (db != null);
    Map<String, String> dbParameters = db.getParameters();
    return dbParameters != null && !StringUtils.isEmpty(dbParameters.get(ReplConst.TARGET_OF_REPLICATION));
  }

  public static boolean isBackgroundThreadsEnabledForRepl(Database db) {
    assert (db != null);
    Map<String, String> dbParameters = db.getParameters();
    return dbParameters != null && !StringUtils.isEmpty(dbParameters.get(ReplConst.REPL_ENABLE_BACKGROUND_THREAD));
  }

  public static boolean checkIfDbNeedsToBeSkipped(Database db) {
    assert (db != null);
    if (isBackgroundThreadsEnabledForRepl(db)) {
      return false;
    } else if (isDbBeingPlannedFailedOver(db)) {
      LOG.info("Skipping all the tables which belong to database: {} as it is being failed over", db.getName());
      return true;
    } else if (isTargetOfReplication(db)) {
      LOG.info("Skipping all the tables which belong to replicated database: {}", db.getName());
      return true;
    }
    return false;
  }

  public static List<String> getReplicationDbProps() {
    return Arrays.stream(ReplConst.class.getDeclaredFields())
            .filter(field -> Modifier.isStatic(field.getModifiers()))
            .map(field -> {
              try {
                String prop = (String) field.get(String.class);
                return prop.replace("\"", "");
              } catch (IllegalAccessException e) {
                LOG.error("Failed to collect replication specific properties. Reason: ", e);
                throw new RuntimeException(e);
              }
            }).collect(Collectors.toList());
  }

  /**
   * Determines whether an table needs to be purged or not.
   *
   * @param table table of interest
   *
   * @return true if external table needs to be purged
   */
  public static boolean isExternalTablePurge(Table table) {
    if (table == null) {
      return false;
    }
    Map<String, String> params = table.getParameters();
    if (params == null) {
      return false;
    }

    return isPropertyTrue(params, EXTERNAL_TABLE_PURGE);
  }

  public static boolean isExternal(Map<String, String> tableParams){
    return isPropertyTrue(tableParams, "EXTERNAL");
  }

  public static boolean isPropertyTrue(Map<String, String> tableParams, String prop) {
    return "TRUE".equalsIgnoreCase(tableParams.get(prop));
  }

  /**
   * Determines whether an table needs to be deleted completely or moved to trash directory.
   *
   * @param tableParams parameters of the table
   *
   * @return true if the table needs to be deleted rather than moved to trash directory
   */
  public static boolean isSkipTrash(Map<String, String> tableParams) {
    if (tableParams == null) {
      return false;
    }
    return isPropertyTrue(tableParams, "skip.trash")
        || isPropertyTrue(tableParams, "auto.purge");
  }

  /** Duplicates AcidUtils; used in a couple places in metastore. */
  public static boolean isInsertOnlyTableParam(Map<String, String> params) {
    String transactionalProp = params.get(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES);
    return (transactionalProp != null && "insert_only".equalsIgnoreCase(transactionalProp));
  }

  public static boolean isNonNativeTable(Table table) {
    if (table == null || table.getParameters() == null) {
      return false;
    }
    return isNonNativeTable(table.getParameters());
  }

  public static boolean isNonNativeTable(Map<String, String> tblProps) {
    return tblProps.get(
            org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE)
            != null;
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
    List<String> pvals = new ArrayList<>(partCols.size());
    for (FieldSchema field : partCols) {
      String val = StringUtils.defaultString(partSpec.get(field.getName()));
      pvals.add(val);
    }
    return pvals;
  }
  public static String makePartNameMatcher(Table table, List<String> partVals, String defaultStr) throws MetaException {
    List<FieldSchema> partCols = table.getPartitionKeys();
    int numPartKeys = partCols.size();
    if (partVals.size() > numPartKeys) {
      throw new MetaException("Incorrect number of partition values."
          + " numPartKeys=" + numPartKeys + ", part_val=" + partVals);
    }
    partCols = partCols.subList(0, partVals.size());
    // Construct a pattern of the form: partKey=partVal/partKey2=partVal2/...
    // where partVal is either the escaped partition value given as input,
    // or a regex of the form ".*"
    // This works because the "=" and "/" separating key names and partition key/values
    // are not escaped.
    String partNameMatcher = Warehouse.makePartName(partCols, partVals, defaultStr);
    // add ".*" to the regex to match anything else afterwards the partial spec.
    if (partVals.size() < numPartKeys) {
      partNameMatcher += defaultStr;
    }
    return partNameMatcher;
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

  public static boolean isArchived(Partition part) {
    Map<String, String> params = part.getParameters();
    return (params != null && "TRUE".equalsIgnoreCase(params.get(hive_metastoreConstants.IS_ARCHIVED)));
  }

  public static Path getOriginalLocation(Partition part) {
    Map<String, String> params = part.getParameters();
    assert(isArchived(part));
    String originalLocation = params.get(hive_metastoreConstants.ORIGINAL_LOCATION);
    assert( originalLocation != null);

    return new Path(originalLocation);
  }

  private static String ARCHIVING_LEVEL = "archiving_level";
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

  /**
   * Read and return the meta store Sasl configuration. Currently it uses the default
   * Hadoop SASL configuration and can be configured using "hadoop.rpc.protection"
   * HADOOP-10211, made a backward incompatible change due to which this call doesn't
   * work with Hadoop 2.4.0 and later.
   * @param conf
   * @return The SASL configuration
   */
  public static Map<String, String> getMetaStoreSaslProperties(Configuration conf, boolean useSSL) {
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

  /**
   * Returns currently known class paths as best effort. For system class loader, this may return
   * In such cases we will anyway create new child class loader in {@link #addToClassPath(ClassLoader cloader, String[] newPaths)
   * so all new class paths will be added and next time we will have a URLClassLoader to work wit
   */
  private static List<URL> getCurrentClassPaths(ClassLoader parentLoader) {
    if(parentLoader instanceof URLClassLoader) {
      return Lists.newArrayList(((URLClassLoader) parentLoader).getURLs());
    } else {
      return Collections.emptyList();
    }
  }

  /**
   * Add new elements to the classpath.
   *
   * @param newPaths
   *          Array of classpath elements
   */
  public static ClassLoader addToClassPath(ClassLoader cloader, String[] newPaths) throws Exception {
    List<URL> curPath = getCurrentClassPaths(cloader);

    for (String onestr : newPaths) {
      URL oneurl = urlFromPathString(onestr);
      if (oneurl != null && !curPath.contains(oneurl)) {
        curPath.add(oneurl);
      }
    }

    return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
      @Override
      public ClassLoader run() {
        return new URLClassLoader(curPath.toArray(new URL[0]), cloader);
      }
    });
  }

  /**
   * Create a URL from a string representing a path to a local file.
   * The path string can be just a path, or can start with file:/, file:///
   * @param onestr  path string
   * @return
   */
  private static URL urlFromPathString(String onestr) {
    URL oneurl = null;
    try {
      if (onestr.startsWith("file:/")) {
        oneurl = new URL(onestr);
      } else {
        oneurl = new File(onestr).toURL();
      }
    } catch (Exception err) {
      LOG.error("Bad URL " + onestr + ", ignoring path");
    }
    return oneurl;
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
      ddl.append(ColumnType.typeToThriftType(col.getType()));
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
   * @param parameters partition level parameters
   * @param tblSchema The table level schema from which this partition should be copied.
   * @return the properties
   */
  public static Properties getPartSchemaFromTableSchema(
      StorageDescriptor sd,
      Map<String, String> parameters,
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
        Joiner.on(",").join(sd.getBucketCols()));
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
            ((key.equals(cols) || key.equals(colTypes) || key.equals(parts) ||
                // Skip Druid and JDBC properties which are used in respective SerDes,
                // since they are also updated after SerDeInfo properties are copied.
                key.startsWith(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.DRUID_CONFIG_PREFIX) ||
                key.startsWith(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.JDBC_CONFIG_PREFIX)))) {
          continue;
        }
        schema.put(key, (param.getValue() != null) ? param.getValue() : StringUtils.EMPTY);
      }

      if (sd.getSerdeInfo().getSerializationLib() != null) {
        schema.setProperty(ColumnType.SERIALIZATION_LIB, sd.getSerdeInfo().getSerializationLib());
      }
    }

    // skipping columns since partition level field schemas are the same as table level's
    // skipping partition keys since it is the same as table level partition keys

    if (parameters != null) {
      for (Map.Entry<String, String> e : parameters.entrySet()) {
        schema.setProperty(e.getKey(), e.getValue());
      }
    }

    return schema;
  }

  private static Properties addCols(Properties schema, List<FieldSchema> cols) {

    StringBuilder colNameBuf = new StringBuilder();
    StringBuilder colTypeBuf = new StringBuilder();
    StringBuilder colComment = new StringBuilder();

    boolean first = true;
    String columnNameDelimiter = getColumnNameDelimiter(cols);
    for (FieldSchema col : cols) {
      if (!first) {
        colNameBuf.append(columnNameDelimiter);
        colTypeBuf.append(":");
        colComment.append(ColumnType.COLUMN_COMMENTS_DELIMITER);
      }
      colNameBuf.append(col.getName());
      colTypeBuf.append(col.getType());
      colComment.append((null != col.getComment()) ? col.getComment() : StringUtils.EMPTY);
      first = false;
    }
    schema.setProperty(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS,
        colNameBuf.toString());
    schema.setProperty(ColumnType.COLUMN_NAME_DELIMITER, columnNameDelimiter);
    String colTypes = colTypeBuf.toString();
    schema.setProperty(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES,
        colTypes);
    schema.setProperty("columns.comments", colComment.toString());

    return schema;

  }

  public static Properties getSchemaWithoutCols(StorageDescriptor sd,
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
    int bucket_cnt = sd.getNumBuckets();
    if (bucket_cnt > 0) {
      schema.setProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.BUCKET_COUNT,
          Integer.toString(bucket_cnt));
    }

    if (sd.getBucketCols() != null && sd.getBucketCols().size() > 0) {
      schema.setProperty(
          org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.BUCKET_FIELD_NAME,
        Joiner.on(",").join(sd.getBucketCols()));
    }
    if (sd.getSerdeInfo() != null) {
      for (Map.Entry<String,String> param : sd.getSerdeInfo().getParameters().entrySet()) {
        schema.put(param.getKey(), (param.getValue() != null) ? param.getValue() : StringUtils.EMPTY);
      }

      if (sd.getSerdeInfo().getSerializationLib() != null) {
        schema.setProperty(ColumnType.SERIALIZATION_LIB, sd .getSerdeInfo().getSerializationLib());
      }
    }

    if (partitionKeys != null) {
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
    }

    if (parameters != null) {
      for (Map.Entry<String, String> e : parameters.entrySet()) {
        // add non-null parameters to the schema
        String key = e.getKey();
        if (!StatsSetupConst.COLUMN_STATS_ACCURATE.equals(key) &&
            !hive_metastoreConstants.DDL_TIME.equals(key) &&
            !StatsSetupConst.TOTAL_SIZE.equals(key) &&
            !StatsSetupConst.RAW_DATA_SIZE.equals(key) &&
            !StatsSetupConst.NUM_FILES.equals(key) &&
            !StatsSetupConst.ROW_COUNT.equals(key) && e.getValue() != null) {
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

    return addCols(getSchemaWithoutCols(sd, parameters, databaseName, tableName, partitionKeys), tblsd.getCols());
  }

  public static String getColumnNameDelimiter(List<FieldSchema> fieldSchemas) {
    // we first take a look if any fieldSchemas contain COMMA
    for (int i = 0; i < fieldSchemas.size(); i++) {
      if (fieldSchemas.get(i).getName().contains(",")) {
        return String.valueOf(ColumnType.COLUMN_COMMENTS_DELIMITER);
      }
    }
    return String.valueOf(',');
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
      List<FieldSchema> fieldSchemas, String delimiter) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < fieldSchemas.size(); i++) {
      if (i > 0) {
        sb.append(delimiter);
      }
      sb.append(fieldSchemas.get(i).getType());
    }
    return sb.toString();
  }

  public static String getColumnTypesFromFieldSchema(
      List<FieldSchema> fieldSchemas) {
    return getColumnTypesFromFieldSchema(fieldSchemas, ",");
  }

  public static String getColumnCommentsFromFieldSchema(List<FieldSchema> fieldSchemas) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < fieldSchemas.size(); i++) {
      if (i > 0) {
        sb.append(ColumnType.COLUMN_COMMENTS_DELIMITER);
      }
      sb.append(fieldSchemas.get(i).getComment());
    }
    return sb.toString();
  }

  public static boolean isMaterializedViewTable(Table table) {
    if (table == null) {
      return false;
    }
    return TableType.MATERIALIZED_VIEW.toString().equals(table.getTableType());
  }

  public static List<String> getColumnNames(List<FieldSchema> schema) {
    List<String> cols = new ArrayList<>(schema.size());
    for (FieldSchema fs : schema) {
      cols.add(fs.getName());
    }
    return cols;
  }

  public static boolean isValidSchedulingPolicy(String str) {
    try {
      parseSchedulingPolicy(str);
      return true;
    } catch (IllegalArgumentException ex) {
    }
    return false;
  }

  public static WMPoolSchedulingPolicy parseSchedulingPolicy(String schedulingPolicy) {
    if (schedulingPolicy == null) {
      return WMPoolSchedulingPolicy.FAIR;
    }
    schedulingPolicy = schedulingPolicy.trim().toUpperCase();
    if ("DEFAULT".equals(schedulingPolicy)) {
      return WMPoolSchedulingPolicy.FAIR;
    }
    return Enum.valueOf(WMPoolSchedulingPolicy.class, schedulingPolicy);
  }

  private static boolean hasCatalogName(String dbName) {
    return dbName != null && dbName.length() > 0 &&
        dbName.charAt(0) == CATALOG_DB_THRIFT_NAME_MARKER;
  }

  /**
   * Given a catalog name and database name cram them together into one string.  This method can
   * be used if you do not know the catalog name, in which case the default catalog will be
   * retrieved from the conf object.  The resulting string can be parsed apart again via
   * {@link #parseDbName(String, Configuration)}.
   * @param catalogName catalog name, can be null if no known.
   * @param dbName database name, can be null or empty.
   * @param conf configuration object, used to determine default catalog if catalogName is null
   * @return one string that contains both.
   */
  public static String prependCatalogToDbName(@Nullable String catalogName, @Nullable String dbName,
                                              Configuration conf) {
    if (catalogName == null) {
      catalogName = getDefaultCatalog(conf);
    }
    StringBuilder buf = new StringBuilder()
        .append(CATALOG_DB_THRIFT_NAME_MARKER)
        .append(catalogName)
        .append(CATALOG_DB_SEPARATOR);
    if (dbName != null) {
      if (dbName.isEmpty()) {
        buf.append(DB_EMPTY_MARKER);
      } else {
        buf.append(dbName);
      }
    }
    return buf.toString();
  }

  /**
   * Given a catalog name and database name, cram them together into one string.  These can be
   * parsed apart again via {@link #parseDbName(String, Configuration)}.
   * @param catalogName catalog name.  This cannot be null.  If this might be null use
   *                    {@link #prependCatalogToDbName(String, String, Configuration)} instead.
   * @param dbName database name.
   * @return one string that contains both.
   */
  public static String prependNotNullCatToDbName(String catalogName, String dbName) {
    assert catalogName != null;
    return prependCatalogToDbName(catalogName, dbName, null);
  }

  /**
   * Prepend the default 'hive' catalog onto the database name.
   * @param dbName database name
   * @param conf configuration object, used to determine default catalog
   * @return one string with the 'hive' catalog name prepended.
   */
  public static String prependCatalogToDbName(String dbName, Configuration conf) {
    return prependCatalogToDbName(null, dbName, conf);
  }

  private final static String[] nullCatalogAndDatabase = {null, null};

  /**
   * Parse the catalog name out of the database name.  If no catalog name is present then the
   * default catalog (as set in configuration file) will be assumed.
   * @param dbName name of the database.  This may or may not contain the catalog name.
   * @param conf configuration object, used to determine the default catalog if it is not present
   *            in the database name.
   * @return an array of two elements, the first being the catalog name, the second the database
   * name.
   * @throws MetaException if the name is not either just a database name or a catalog plus
   * database name with the proper delimiters.
   */
  public static String[] parseDbName(String dbName, Configuration conf) throws MetaException {
    if (dbName == null) {
      return Arrays.copyOf(nullCatalogAndDatabase, nullCatalogAndDatabase.length);
    }
    if (hasCatalogName(dbName)) {
      if (dbName.endsWith(CATALOG_DB_SEPARATOR)) {
        // This means the DB name is null
        return new String[] {dbName.substring(1, dbName.length() - 1), null};
      } else if (dbName.endsWith(DB_EMPTY_MARKER)) {
        // This means the DB name is empty
        return new String[] {dbName.substring(1, dbName.length() - DB_EMPTY_MARKER.length() - 1), ""};
      }
      String[] names = dbName.substring(1).split(CATALOG_DB_SEPARATOR, 2);
      if (names.length != 2) {
        throw new MetaException(dbName + " is prepended with the catalog marker but does not " +
            "appear to have a catalog name in it");
      }
      return names;
    } else {
      return new String[] {getDefaultCatalog(conf), dbName};
    }
  }

  /**
   * Position in the array returned by {@link #parseDbName} that has the catalog name.
   */
  public static final int CAT_NAME = 0;
  /**
   * Position in the array returned by {@link #parseDbName} that has the database name.
   */
  public static final int DB_NAME = 1;

  public static String getDefaultCatalog(Configuration conf) {
    if (conf == null) {
      LOG.warn("Configuration is null, so going with default catalog.");
      return Warehouse.DEFAULT_CATALOG_NAME;
    }
    String catName = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CATALOG_DEFAULT);
    if (catName == null || "".equals(catName)) {
      catName = Warehouse.DEFAULT_CATALOG_NAME;
    }
    return catName;
  }

  public static boolean isView(Table table) {
    if (table == null) {
      return false;
    }
    return TableType.VIRTUAL_VIEW.toString().equals(table.getTableType());
  }

  /**
   * filters a given map with predicate provided. All entries of map whose key matches with
   * predicate will be removed. Expects map to be modifiable and does the operation on actual map,
   * so does not return a copy of filtered map.
   * @param map A map of String key-value pairs
   * @param predicate Predicate with pattern to filter the map
   */
  public static <T> void filterMapKeys(Map<String, T> map, Predicate<String> predicate) {
    if (map == null) {
      return;
    }
    map.entrySet().removeIf(entry -> predicate.test(entry.getKey()));
  }

  /**
   * filters a given map with list of predicates. All entries of map whose key matches with any
   * predicate will be removed. Expects map to be modifiable and does the operation on actual map,
   * so does not return a copy of filtered map.
   * @param map A map of String key-value pairs
   * @param predicates List of predicates with patterns to filter the map
   */
  public static <T> void filterMapkeys(Map<String, T> map, List<Predicate<String>> predicates) {
    if (map == null) {
      return;
    }
    filterMapKeys(map, predicates.stream().reduce(Predicate::or).orElse(x -> false));
  }

  /**
   * Compile a list of regex patterns and collect them as Predicates.
   * @param patterns List of regex patterns to be compiled
   * @return a List of Predicate created by compiling the regex patterns
   */
  public static List<Predicate<String>> compilePatternsToPredicates(List<String> patterns) {
    return patterns.stream().map(pattern -> compile(pattern).asPredicate()).collect(Collectors.toList());
  }

  /**
   * Get order specs from a represented string.
   * @param order specified in partColIndex[,partColIndex]*:[-|\+]+ pattern
   * @return the order specs
   */
  public static List<Object[]> makeOrderSpecs(String order) {
    if (StringUtils.isBlank(order) || order.split(":").length != 2) {
      return new ArrayList<Object[]>();
    }
    String[] parts = order.split(":");
    String[] poses = parts[0].split(",");
    char[] chars = parts[1].toCharArray();
    List<Object[]> orderSpecs = new ArrayList<Object[]>(chars.length);
    if (poses.length != chars.length) {
      throw new IllegalArgumentException("The length of partition keys and sort order" +
          " do not mismatch, order: " + order);
    }
    for (int i = 0; i < poses.length; i++) {
      Object[] spec = new Object[2];
      spec[0] = Integer.parseInt(poses[i]);
      spec[1] = ('+' == chars[i]) ? "ASC" : "DESC";
      orderSpecs.add(spec);
    }
    return orderSpecs;
  }

  public static void addPartitonSpecsToList(PartitionsSpecByExprResult r, List<PartitionSpec> result) {
    result.addAll(r.getPartitionsSpec());
  }

  public static boolean hasUnknownPartitions(PartitionsSpecByExprResult r) {
    return !r.isSetHasUnknownPartitions() || r.isHasUnknownPartitions();
  }

  public static TableName getTableNameFor(Table table) {
    return TableName.fromString(table.getTableName().toLowerCase(), table.getCatName().toLowerCase(), table.getDbName().toLowerCase(), null);
  }

  /**
   * Because TABLE_NO_AUTO_COMPACT was originally assumed to be NO_AUTO_COMPACT and then was moved
   * to no_auto_compact, we need to check it in both cases.
   * Check the database level no_auto_compact , if present it is given priority else table level no_auto_compact is considered.
   */
  public static boolean isNoAutoCompactSet(Map<String, String> dbParameters, Map<String, String> tblParameters) {
    String dbNoAutoCompact = getNoAutoCompact(dbParameters);
    if (dbNoAutoCompact == null) {
      LOG.debug("Using table configuration '" + hive_metastoreConstants.NO_AUTO_COMPACT + "' for compaction");
      String noAutoCompact = getNoAutoCompact(tblParameters);
      return Boolean.parseBoolean(noAutoCompact);
    }
    LOG.debug("Using database configuration '" + hive_metastoreConstants.NO_AUTO_COMPACT + "' for compaction");
    return Boolean.parseBoolean(dbNoAutoCompact);
  }

  /**
   * Get no_auto_compact property by checking in both lower and upper cases
   * @param parameters
   * @return true/false if set, null if there is no NO_AUTO_COMPACT set in database level config,
   */
  public static String getNoAutoCompact(Map<String, String> parameters) {
    String noAutoCompact = parameters.get(hive_metastoreConstants.NO_AUTO_COMPACT);
    if (noAutoCompact == null) {
      return parameters.get(hive_metastoreConstants.NO_AUTO_COMPACT.toUpperCase());
    }
    return noAutoCompact;
  }

  public static String getHostFromId(String id) {
    if (id == null) {
      return NO_VAL;
    }
    int lastDash = id.lastIndexOf('-');
    return id.substring(0, lastDash > -1 ? lastDash : id.length());
  }

  public static String getThreadIdFromId(String id) {
    if (id == null) {
      return NO_VAL;
    }
    return id.substring(id.lastIndexOf('-') + 1);
  }

  public enum FailoverEndpoint {
    /**
     * EndPoint to specify nature of database for which failover is initiated.
     */
    SOURCE, TARGET;
  }

  public static boolean isNoCleanUpSet(Map<String, String> parameters) {
    String noCleanUp = parameters.get(hive_metastoreConstants.NO_CLEANUP);
    if (noCleanUp == null) {
      noCleanUp = parameters.get(hive_metastoreConstants.NO_CLEANUP.toUpperCase());
    }
    return noCleanUp != null && noCleanUp.equalsIgnoreCase("true");
  }

  public static GetPartitionsByNamesRequest convertToGetPartitionsByNamesRequest(String dbName, String tblName,
      List<String> partNames) {
    GetPartitionsByNamesRequest result = new GetPartitionsByNamesRequest(dbName, tblName);
    result.setNames(partNames);
    result.setGet_col_stats(false);
    return result;
  }

  public static GetPartitionsByNamesRequest convertToGetPartitionsByNamesRequest(String dbName, String tblName,
      List<String> partNames, boolean getColStats, String engine, String validWriteIdList, Long tableId) {
    GetPartitionsByNamesRequest result = new GetPartitionsByNamesRequest(dbName, tblName);
    result.setNames(partNames);
    result.setGet_col_stats(getColStats);
    result.setEngine(engine);
    result.setValidWriteIdList(validWriteIdList);
    if (tableId != null) {
      result.setId(tableId);
    }
    return result;
  }

  public static <T> T createThriftPartitionsReq(Class<T> clazz, Configuration conf, T... deepCopy) {
    final T req;
    if (deepCopy != null && deepCopy.length == 1) {
      assert clazz.isAssignableFrom(deepCopy[0].getClass());
      req = JavaUtils.newInstance(clazz, new Class[]{clazz}, deepCopy);
    } else {
      req = JavaUtils.newInstance(clazz);
    }
    JavaUtils.setField(req, "setSkipColumnSchemaForPartition", new Class[]{boolean.class},
        MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METASTORE_CLIENT_FIELD_SCHEMA_FOR_PARTITIONS));
    JavaUtils.setField(req, "setIncludeParamKeyPattern", new Class[]{String.class},
        MetastoreConf.getAsString(conf, MetastoreConf.ConfVars.METASTORE_PARTITIONS_PARAMETERS_INCLUDE_PATTERN));
    JavaUtils.setField(req, "setExcludeParamKeyPattern", new Class[]{String.class},
        MetastoreConf.getAsString(conf, MetastoreConf.ConfVars.METASTORE_PARTITIONS_PARAMETERS_EXCLUDE_PATTERN));
    return req;
  }

  /**
   * The config parameter can be like "path", "/path", "/path/", "path/*", "/path1/path2/*" and so on.
   * httpPath should end up as "/*", "/path/*" or "/path1/../pathN/*"
   * @param httpPath
   * @return
   */
  public static String getHttpPath(String httpPath) {
    if (httpPath == null || httpPath.equals("")) {
      httpPath = "/*";
    } else {
      if (!httpPath.startsWith("/")) {
        httpPath = "/" + httpPath;
      }
      if (httpPath.endsWith("/")) {
        httpPath = httpPath + "*";
      }
      if (!httpPath.endsWith("/*")) {
        httpPath = httpPath + "/*";
      }
    }
    return httpPath;
  }
}
