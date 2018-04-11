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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.security.auth.login.LoginException;

import com.google.common.collect.Maps;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.security.DelegationTokenIdentifier;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hive.hcatalog.data.Pair;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;
import org.apache.hive.hcatalog.mapreduce.FosterStorageHandler;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.InputJobInfo;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.hive.hcatalog.mapreduce.PartInfo;
import org.apache.hive.hcatalog.mapreduce.StorerInfo;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HCatUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HCatUtil.class);
  private static final HashMap<String, String> hiveConfCodeDefaults;
  private static volatile HiveClientCache hiveClientCache;

  static {
    // Load all of the default config values from HiveConf.
    hiveConfCodeDefaults = Maps.newHashMapWithExpectedSize(HiveConf.ConfVars.values().length);
    for (HiveConf.ConfVars var : HiveConf.ConfVars.values()) {
      hiveConfCodeDefaults.put(var.toString(), var.getDefaultValue());
    }
  }

  public static boolean checkJobContextIfRunningFromBackend(JobContext j) {
    if (j.getConfiguration().get("pig.job.converted.fetch", "").equals("") &&
          j.getConfiguration().get("mapred.task.id", "").equals("") &&
          !("true".equals(j.getConfiguration().get("pig.illustrating")))) {
      return false;
    }
    return true;
  }

  public static String serialize(Serializable obj) throws IOException {
    if (obj == null) {
      return "";
    }
    try {
      ByteArrayOutputStream serialObj = new ByteArrayOutputStream();
      ObjectOutputStream objStream = new ObjectOutputStream(serialObj);
      objStream.writeObject(obj);
      objStream.close();
      return encodeBytes(serialObj.toByteArray());
    } catch (Exception e) {
      throw new IOException("Serialization error: " + e.getMessage(), e);
    }
  }

  public static Object deserialize(String str) throws IOException {
    if (str == null || str.length() == 0) {
      return null;
    }
    try {
      ByteArrayInputStream serialObj = new ByteArrayInputStream(
        decodeBytes(str));
      ObjectInputStream objStream = new ObjectInputStream(serialObj);
      return objStream.readObject();
    } catch (Exception e) {
      throw new IOException("Deserialization error: " + e.getMessage(), e);
    }
  }

  public static String encodeBytes(byte[] bytes) {
    return new String(Base64.encodeBase64(bytes, false, false));
  }

  public static byte[] decodeBytes(String str) {
    return Base64.decodeBase64(str.getBytes());
  }

  public static List<HCatFieldSchema> getHCatFieldSchemaList(
    FieldSchema... fields) throws HCatException {
    List<HCatFieldSchema> result = new ArrayList<HCatFieldSchema>(
      fields.length);

    for (FieldSchema f : fields) {
      result.add(HCatSchemaUtils.getHCatFieldSchema(f));
    }

    return result;
  }

  public static List<HCatFieldSchema> getHCatFieldSchemaList(
    List<FieldSchema> fields) throws HCatException {
    if (fields == null) {
      return null;
    } else {
      List<HCatFieldSchema> result = new ArrayList<HCatFieldSchema>();
      for (FieldSchema f : fields) {
        result.add(HCatSchemaUtils.getHCatFieldSchema(f));
      }
      return result;
    }
  }

  public static HCatSchema extractSchema(Table table) throws HCatException {
    return new HCatSchema(HCatUtil.getHCatFieldSchemaList(table.getCols()));
  }

  public static HCatSchema extractSchema(Partition partition) throws HCatException {
    return new HCatSchema(HCatUtil.getHCatFieldSchemaList(partition.getCols()));
  }

  public static List<FieldSchema> getFieldSchemaList(
    List<HCatFieldSchema> hcatFields) {
    if (hcatFields == null) {
      return null;
    } else {
      List<FieldSchema> result = new ArrayList<FieldSchema>();
      for (HCatFieldSchema f : hcatFields) {
        result.add(HCatSchemaUtils.getFieldSchema(f));
      }
      return result;
    }
  }

  public static Table getTable(IMetaStoreClient client, String dbName, String tableName)
    throws NoSuchObjectException, TException, MetaException {
    return new Table(client.getTable(dbName, tableName));
  }

  public static HCatSchema getTableSchemaWithPtnCols(Table table) throws IOException {
    HCatSchema tableSchema = new HCatSchema(HCatUtil.getHCatFieldSchemaList(table.getCols()));

    if (table.getPartitionKeys().size() != 0) {

      // add partition keys to table schema
      // NOTE : this assumes that we do not ever have ptn keys as columns
      // inside the table schema as well!
      for (FieldSchema fs : table.getPartitionKeys()) {
        tableSchema.append(HCatSchemaUtils.getHCatFieldSchema(fs));
      }
    }
    return tableSchema;
  }

  /**
   * return the partition columns from a table instance
   *
   * @param table the instance to extract partition columns from
   * @return HCatSchema instance which contains the partition columns
   * @throws IOException
   */
  public static HCatSchema getPartitionColumns(Table table) throws IOException {
    HCatSchema cols = new HCatSchema(new LinkedList<HCatFieldSchema>());
    if (table.getPartitionKeys().size() != 0) {
      for (FieldSchema fs : table.getPartitionKeys()) {
        cols.append(HCatSchemaUtils.getHCatFieldSchema(fs));
      }
    }
    return cols;
  }

  /**
   * Validate partition schema, checks if the column types match between the
   * partition and the existing table schema. Returns the list of columns
   * present in the partition but not in the table.
   *
   * @param table the table
   * @param partitionSchema the partition schema
   * @return the list of newly added fields
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public static List<FieldSchema> validatePartitionSchema(Table table,
                              HCatSchema partitionSchema) throws IOException {
    Map<String, FieldSchema> partitionKeyMap = new HashMap<String, FieldSchema>();

    for (FieldSchema field : table.getPartitionKeys()) {
      partitionKeyMap.put(field.getName().toLowerCase(), field);
    }

    List<FieldSchema> tableCols = table.getCols();
    List<FieldSchema> newFields = new ArrayList<FieldSchema>();

    for (int i = 0; i < partitionSchema.getFields().size(); i++) {

      FieldSchema field = HCatSchemaUtils.getFieldSchema(partitionSchema
        .getFields().get(i));

      FieldSchema tableField;
      if (i < tableCols.size()) {
        tableField = tableCols.get(i);

        if (!tableField.getName().equalsIgnoreCase(field.getName())) {
          throw new HCatException(
            ErrorType.ERROR_SCHEMA_COLUMN_MISMATCH,
            "Expected column <" + tableField.getName()
              + "> at position " + (i + 1)
              + ", found column <" + field.getName()
              + ">");
        }
      } else {
        tableField = partitionKeyMap.get(field.getName().toLowerCase());

        if (tableField != null) {
          throw new HCatException(
            ErrorType.ERROR_SCHEMA_PARTITION_KEY, "Key <"
            + field.getName() + ">");
        }
      }

      if (tableField == null) {
        // field present in partition but not in table
        newFields.add(field);
      } else {
        // field present in both. validate type has not changed
        TypeInfo partitionType = TypeInfoUtils
          .getTypeInfoFromTypeString(field.getType());
        TypeInfo tableType = TypeInfoUtils
          .getTypeInfoFromTypeString(tableField.getType());

        if (!partitionType.equals(tableType)) {
          String msg =
            "Column <"
            + field.getName() + ">, expected <"
            + tableType.getTypeName() + ">, got <"
            + partitionType.getTypeName() + ">";
          LOG.warn(msg);
          throw new HCatException(ErrorType.ERROR_SCHEMA_TYPE_MISMATCH, msg);
        }
      }
    }

    return newFields;
  }

  /**
   * Test if the first FsAction is more permissive than the second. This is
   * useful in cases where we want to ensure that a file owner has more
   * permissions than the group they belong to, for eg. More completely(but
   * potentially more cryptically) owner-r >= group-r >= world-r : bitwise
   * and-masked with 0444 => 444 >= 440 >= 400 >= 000 owner-w >= group-w >=
   * world-w : bitwise and-masked with &0222 => 222 >= 220 >= 200 >= 000
   * owner-x >= group-x >= world-x : bitwise and-masked with &0111 => 111 >=
   * 110 >= 100 >= 000
   *
   * @return true if first FsAction is more permissive than the second, false
   *         if not.
   */
  public static boolean validateMorePermissive(FsAction first, FsAction second) {
    if ((first == FsAction.ALL) || (second == FsAction.NONE)
      || (first == second)) {
      return true;
    }
    switch (first) {
    case READ_EXECUTE:
      return ((second == FsAction.READ) || (second == FsAction.EXECUTE));
    case READ_WRITE:
      return ((second == FsAction.READ) || (second == FsAction.WRITE));
    case WRITE_EXECUTE:
      return ((second == FsAction.WRITE) || (second == FsAction.EXECUTE));
    }
    return false;
  }

  /**
   * Ensure that read or write permissions are not granted without also
   * granting execute permissions. Essentially, r-- , rw- and -w- are invalid,
   * r-x, -wx, rwx, ---, --x are valid
   *
   * @param perms The FsAction to verify
   * @return true if the presence of read or write permission is accompanied
   *         by execute permissions
   */
  public static boolean validateExecuteBitPresentIfReadOrWrite(FsAction perms) {
    if ((perms == FsAction.READ) || (perms == FsAction.WRITE)
      || (perms == FsAction.READ_WRITE)) {
      return false;
    }
    return true;
  }

  public static Token<org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier> getJobTrackerDelegationToken(
    Configuration conf, String userName) throws Exception {
    // LOG.info("getJobTrackerDelegationToken("+conf+","+userName+")");
    JobClient jcl = new JobClient(new JobConf(conf, HCatOutputFormat.class));
    Token<org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier> t = jcl
      .getDelegationToken(new Text(userName));
    // LOG.info("got "+t);
    return t;

    // return null;
  }

  public static Token<? extends AbstractDelegationTokenIdentifier> extractThriftToken(
    String tokenStrForm, String tokenSignature) throws MetaException,
    TException, IOException {
    // LOG.info("extractThriftToken("+tokenStrForm+","+tokenSignature+")");
    Token<? extends AbstractDelegationTokenIdentifier> t = new Token<DelegationTokenIdentifier>();
    t.decodeFromUrlString(tokenStrForm);
    t.setService(new Text(tokenSignature));
    // LOG.info("returning "+t);
    return t;
  }

  /**
   * Create an instance of a storage handler defined in storerInfo. If one cannot be found
   * then FosterStorageHandler is used to encapsulate the InputFormat, OutputFormat and SerDe.
   * This StorageHandler assumes the other supplied storage artifacts are for a file-based storage system.
   * @param conf job's configuration will be used to configure the Configurable StorageHandler
   * @param storerInfo StorerInfo to definining the StorageHandler and InputFormat, OutputFormat and SerDe
   * @return storageHandler instance
   * @throws IOException
   */
  public static HiveStorageHandler getStorageHandler(Configuration conf, StorerInfo storerInfo) throws IOException {
    return getStorageHandler(conf,
      storerInfo.getStorageHandlerClass(),
      storerInfo.getSerdeClass(),
      storerInfo.getIfClass(),
      storerInfo.getOfClass());
  }

  public static HiveStorageHandler getStorageHandler(Configuration conf, PartInfo partitionInfo) throws IOException {
    return HCatUtil.getStorageHandler(
      conf,
      partitionInfo.getStorageHandlerClassName(),
      partitionInfo.getSerdeClassName(),
      partitionInfo.getInputFormatClassName(),
      partitionInfo.getOutputFormatClassName());
  }

  /**
   * Create an instance of a storage handler. If storageHandler == null,
   * then surrrogate StorageHandler is used to encapsulate the InputFormat, OutputFormat and SerDe.
   * This StorageHandler assumes the other supplied storage artifacts are for a file-based storage system.
   * @param conf job's configuration will be used to configure the Configurable StorageHandler
   * @param storageHandler fully qualified class name of the desired StorageHandle instance
   * @param serDe fully qualified class name of the desired SerDe instance
   * @param inputFormat fully qualified class name of the desired InputFormat instance
   * @param outputFormat fully qualified class name of the desired outputFormat instance
   * @return storageHandler instance
   * @throws IOException
   */
  public static HiveStorageHandler getStorageHandler(Configuration conf,
                             String storageHandler,
                             String serDe,
                             String inputFormat,
                             String outputFormat)
    throws IOException {

    if ((storageHandler == null) || (storageHandler.equals(FosterStorageHandler.class.getName()))) {
      try {
        FosterStorageHandler fosterStorageHandler =
          new FosterStorageHandler(inputFormat, outputFormat, serDe);
        fosterStorageHandler.setConf(conf);
        return fosterStorageHandler;
      } catch (ClassNotFoundException e) {
        throw new IOException("Failed to load "
          + "foster storage handler", e);
      }
    }

    try {
      Class<? extends HiveStorageHandler> handlerClass =
        (Class<? extends HiveStorageHandler>) Class
          .forName(storageHandler, true, Utilities.getSessionSpecifiedClassLoader());
      return (HiveStorageHandler) ReflectionUtils.newInstance(
        handlerClass, conf);
    } catch (ClassNotFoundException e) {
      throw new IOException("Error in loading storage handler."
        + e.getMessage(), e);
    }
  }

  public static Pair<String, String> getDbAndTableName(String tableName) throws IOException {
    String[] dbTableNametokens = tableName.split("\\.");
    if (dbTableNametokens.length == 1) {
      return new Pair<String, String>(Warehouse.DEFAULT_DATABASE_NAME, tableName);
    } else if (dbTableNametokens.length == 2) {
      return new Pair<String, String>(dbTableNametokens[0], dbTableNametokens[1]);
    } else {
      throw new IOException("tableName expected in the form "
        + "<databasename>.<table name> or <table name>. Got " + tableName);
    }
  }

  public static Map<String, String>
  getInputJobProperties(HiveStorageHandler storageHandler,
      InputJobInfo inputJobInfo) {
    Properties props = inputJobInfo.getTableInfo().getStorerInfo().getProperties();
    props.put(serdeConstants.SERIALIZATION_LIB,storageHandler.getSerDeClass().getName());
    TableDesc tableDesc = new TableDesc(storageHandler.getInputFormatClass(),
      storageHandler.getOutputFormatClass(),props);
    if (tableDesc.getJobProperties() == null) {
      tableDesc.setJobProperties(new HashMap<String, String>());
    }

    Properties mytableProperties = tableDesc.getProperties();
    mytableProperties.setProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_NAME,inputJobInfo.getDatabaseName()+ "." + inputJobInfo.getTableName());

    Map<String, String> jobProperties = new HashMap<String, String>();
    try {
      tableDesc.getJobProperties().put(
        HCatConstants.HCAT_KEY_JOB_INFO,
        HCatUtil.serialize(inputJobInfo));

      storageHandler.configureInputJobProperties(tableDesc,
        jobProperties);

    } catch (IOException e) {
      throw new IllegalStateException(
        "Failed to configure StorageHandler", e);
    }

    return jobProperties;
  }

  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  public static void
  configureOutputStorageHandler(HiveStorageHandler storageHandler,
                  Configuration conf,
                  OutputJobInfo outputJobInfo) {
    //TODO replace IgnoreKeyTextOutputFormat with a
    //HiveOutputFormatWrapper in StorageHandler
    Properties props = outputJobInfo.getTableInfo().getStorerInfo().getProperties();
    props.put(serdeConstants.SERIALIZATION_LIB,storageHandler.getSerDeClass().getName());
    TableDesc tableDesc = new TableDesc(storageHandler.getInputFormatClass(),
      IgnoreKeyTextOutputFormat.class,props);
    if (tableDesc.getJobProperties() == null)
      tableDesc.setJobProperties(new HashMap<String, String>());
    for (Map.Entry<String, String> el : conf) {
      tableDesc.getJobProperties().put(el.getKey(), el.getValue());
    }

    Properties mytableProperties = tableDesc.getProperties();
    mytableProperties.setProperty(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_NAME,
        outputJobInfo.getDatabaseName()+ "." + outputJobInfo.getTableName());

    Map<String, String> jobProperties = new HashMap<String, String>();
    try {
      tableDesc.getJobProperties().put(
        HCatConstants.HCAT_KEY_OUTPUT_INFO,
        HCatUtil.serialize(outputJobInfo));

      storageHandler.configureOutputJobProperties(tableDesc,
        jobProperties);

      Map<String, String> tableJobProperties = tableDesc.getJobProperties();
      if (tableJobProperties != null) {
        if (tableJobProperties.containsKey(HCatConstants.HCAT_KEY_OUTPUT_INFO)) {
          String jobString = tableJobProperties.get(HCatConstants.HCAT_KEY_OUTPUT_INFO);
          if (jobString != null) {
            if  (!jobProperties.containsKey(HCatConstants.HCAT_KEY_OUTPUT_INFO)) {
              jobProperties.put(HCatConstants.HCAT_KEY_OUTPUT_INFO,
                  tableJobProperties.get(HCatConstants.HCAT_KEY_OUTPUT_INFO));
            }
          }
        }
      }
      for (Map.Entry<String, String> el : jobProperties.entrySet()) {
        conf.set(el.getKey(), el.getValue());
      }
    } catch (IOException e) {
      throw new IllegalStateException(
        "Failed to configure StorageHandler", e);
    }
  }

  /**
   * Replace the contents of dest with the contents of src
   * @param src
   * @param dest
   */
  public static void copyConf(Configuration src, Configuration dest) {
    dest.clear();
    for (Map.Entry<String, String> el : src) {
      dest.set(el.getKey(), el.getValue());
    }
  }

  /**
   * Get or create a hive client depending on whether it exits in cache or not
   * @param hiveConf The hive configuration
   * @return the client
   * @throws MetaException When HiveMetaStoreClient couldn't be created
   * @throws IOException
   */
  public static IMetaStoreClient getHiveMetastoreClient(HiveConf hiveConf)
      throws MetaException, IOException {

    if (hiveConf.getBoolean(HCatConstants.HCAT_HIVE_CLIENT_DISABLE_CACHE, false)){
      // If cache is disabled, don't use it.
      return HiveClientCache.getNonCachedHiveMetastoreClient(hiveConf);
    }

    // Singleton behaviour: create the cache instance if required.
    if (hiveClientCache == null) {
      synchronized (IMetaStoreClient.class) {
        if (hiveClientCache == null) {
          hiveClientCache = new HiveClientCache(hiveConf);
        }
      }
    }
    try {
      return hiveClientCache.get(hiveConf);
    } catch (LoginException e) {
      throw new IOException("Couldn't create hiveMetaStoreClient, Error getting UGI for user", e);
    }
  }

  /**
   * Get or create a hive client depending on whether it exits in cache or not.
   * @Deprecated : use {@link #getHiveMetastoreClient(HiveConf)} instead.
   * This was deprecated in Hive 1.2, slated for removal in two versions
   * (i.e. 1.2 & 1.3(projected) will have it, but it will be removed after that)
   * @param hiveConf The hive configuration
   * @return the client
   * @throws MetaException When HiveMetaStoreClient couldn't be created
   * @throws IOException
   */
  @Deprecated
  public static HiveMetaStoreClient getHiveClient(HiveConf hiveConf) throws MetaException, IOException {
    LOG.warn("HCatUtil.getHiveClient is unsafe and can be a resource leak depending on HMSC "
        + "implementation and caching mechanism. Use HCatUtil.getHiveMetastoreClient instead.");

    return new HiveMetaStoreClient(hiveConf);
  }

  public static void closeHiveClientQuietly(IMetaStoreClient client) {
    try {
      if (client != null)
        client.close();
    } catch (Exception e) {
      LOG.debug("Error closing metastore client. Ignored the error.", e);
    }
  }

  private static Configuration getHiveSiteContentsFromClasspath() {
    Configuration configuration = new Configuration(false); // Don't load defaults.
    configuration.addResource("hive-site.xml"); // NOTE: hive-site.xml is only available on client, not AM.
    return configuration;
  }

  private static Properties getHiveSiteOverrides(Configuration jobConf) {
    return getHiveSiteOverrides(getHiveSiteContentsFromClasspath(), jobConf);
  }

  /**
   * Returns the hive-site.xml config settings which do not appear in <code>jobConf<code> or
   * the hive-site.xml config settings which appear in <code>jobConf<code>, but have a
   * different value than HiveConf code defaults.
   * @param hiveSite the config settings as found in the hive-site.xml only.
   * @param jobConf the config settings used to launch the job.
   * @return the set difference between hiveSite and jobConf.
   */
  private static Properties getHiveSiteOverrides(Configuration hiveSite, Configuration jobConf) {
    // return (hiveSite - jobConf);
    Properties difference = new Properties();
    for (Map.Entry<String,String> keyValue : hiveSite) {
      String key = keyValue.getKey();
      String hiveSiteValue = keyValue.getValue();
      String jobConfValue = jobConf.getRaw(key);

      if (jobConfValue == null) {
        difference.put(key, hiveSiteValue);
      } else if (hiveConfCodeDefaults.containsKey(key)) {
        // Necessary to compare against HiveConf defaults as hive-site.xml is not available on task nodes (like AM).
        if (! jobConfValue.equals(hiveConfCodeDefaults.get(key))) {
          difference.put(key, jobConfValue);
        }
      }
    }

    LOG.info("Configuration differences=" + difference);

    return difference;
  }

  public static HiveConf getHiveConf(Configuration conf)
    throws IOException {

    HiveConf hiveConf = new HiveConf(conf, HCatUtil.class);

    //copy the hive conf into the job conf and restore it
    //in the backend context
    if (StringUtils.isBlank(conf.get(HCatConstants.HCAT_KEY_HIVE_CONF))) {
      // Called once on the client.
      LOG.info(HCatConstants.HCAT_KEY_HIVE_CONF + " not set. Generating configuration differences.");

      Properties differences = getHiveSiteOverrides(conf);

      // Must set this key even if differences is empty otherwise client and AM will attempt
      // to set this multiple times.
      conf.set(HCatConstants.HCAT_KEY_HIVE_CONF, HCatUtil.serialize(differences));
    } else {
      // Called one or more times on the client and AM.
      LOG.info(HCatConstants.HCAT_KEY_HIVE_CONF + " is set. Applying configuration differences.");

      Properties properties = (Properties) HCatUtil.deserialize(
          conf.get(HCatConstants.HCAT_KEY_HIVE_CONF));

      storePropertiesToHiveConf(properties, hiveConf);
    }

    if (conf.get(HCatConstants.HCAT_KEY_TOKEN_SIGNATURE) != null) {
      hiveConf.setVar(HiveConf.ConfVars.METASTORE_TOKEN_SIGNATURE,
        conf.get(HCatConstants.HCAT_KEY_TOKEN_SIGNATURE));
    }

    return hiveConf;
  }

  public static HiveConf storePropertiesToHiveConf(Properties properties, HiveConf hiveConf)
      throws IOException {
    for (Map.Entry<Object, Object> prop : properties.entrySet()) {
      if (prop.getValue() instanceof String) {
        hiveConf.set((String) prop.getKey(), (String) prop.getValue());
      } else if (prop.getValue() instanceof Integer) {
        hiveConf.setInt((String) prop.getKey(), (Integer) prop.getValue());
      } else if (prop.getValue() instanceof Boolean) {
        hiveConf.setBoolean((String) prop.getKey(), (Boolean) prop.getValue());
      } else if (prop.getValue() instanceof Long) {
        hiveConf.setLong((String) prop.getKey(), (Long) prop.getValue());
      } else if (prop.getValue() instanceof Float) {
        hiveConf.setFloat((String) prop.getKey(), (Float) prop.getValue());
      } else {
        LOG.warn("Unsupported type: key=" + prop.getKey() + " value=" + prop.getValue());
      }
    }
    return hiveConf;
  }

  public static JobConf getJobConfFromContext(JobContext jobContext) {
    JobConf jobConf;
    // we need to convert the jobContext into a jobConf
    // 0.18 jobConf (Hive) vs 0.20+ jobContext (HCat)
    // begin conversion..
    jobConf = new JobConf(jobContext.getConfiguration());
    // ..end of conversion


    return jobConf;
  }

  // Retrieve settings in HiveConf that aren't also set in the JobConf.
  public static Map<String,String> getHCatKeyHiveConf(JobConf conf) {
    try {
      Properties properties = null;

      if (! StringUtils.isBlank(conf.get(HCatConstants.HCAT_KEY_HIVE_CONF))) {
        properties = (Properties) HCatUtil.deserialize(
            conf.get(HCatConstants.HCAT_KEY_HIVE_CONF));

        LOG.info(HCatConstants.HCAT_KEY_HIVE_CONF + " is set. Using differences=" + properties);
      } else {
        LOG.info(HCatConstants.HCAT_KEY_HIVE_CONF + " not set. Generating configuration differences.");

        properties = getHiveSiteOverrides(conf);
      }

      // This method may not be safe as it can throw an NPE if a key or value is null.
      return Maps.fromProperties(properties);
    }
    catch (IOException e) {
      throw new IllegalStateException("Failed to deserialize hive conf", e);
    }
  }

  public static void copyJobPropertiesToJobConf(
    Map<String, String> jobProperties, JobConf jobConf) {
    for (Map.Entry<String, String> entry : jobProperties.entrySet()) {
      jobConf.set(entry.getKey(), entry.getValue());
    }
  }

  public static boolean isHadoop23() {
    String version = org.apache.hadoop.util.VersionInfo.getVersion();
    if (version.matches("\\b0\\.23\\..+\\b")||version.matches("\\b2\\..*")||version.matches("\\b3\\..*"))
      return true;
    return false;
  }
  /**
   * Used by various tests to make sure the path is safe for Windows
   */
  public static String makePathASafeFileName(String filePath) {
    return new File(filePath).getPath().replaceAll("\\\\", "/");
  }
  public static void assertNotNull(Object t, String msg, Logger logger) {
    if(t == null) {
      if(logger != null) {
        logger.warn(msg);
      }
      throw new IllegalArgumentException(msg);
    }
  }
}
