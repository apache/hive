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

package org.apache.hadoop.hive.metastore;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.HdfsUtils;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hive.metastore.ReplChangeManager.RecycleType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This class represents a warehouse where data of Hive tables is stored
 */
public class Warehouse {
  public static final String DEFAULT_DATABASE_NAME = "default";
  public static final String DEFAULT_DATABASE_COMMENT = "Default Hive database";
  public static final String DEFAULT_SERIALIZATION_FORMAT = "1";
  public static final String DATABASE_WAREHOUSE_SUFFIX = ".db";

  private Path whRoot;
  private final Configuration conf;
  private final String whRootString;

  public static final Logger LOG = LoggerFactory.getLogger("hive.metastore.warehouse");

  private MetaStoreFS fsHandler = null;
  private boolean storageAuthCheck = false;
  private ReplChangeManager cm = null;

  public Warehouse(Configuration conf) throws MetaException {
    this.conf = conf;
    whRootString = MetastoreConf.getVar(conf, ConfVars.WAREHOUSE);
    if (StringUtils.isBlank(whRootString)) {
      throw new MetaException(ConfVars.WAREHOUSE.getVarname()
          + " is not set in the config or blank");
    }
    fsHandler = getMetaStoreFsHandler(conf);
    cm = ReplChangeManager.getInstance(conf);
    storageAuthCheck = MetastoreConf.getBoolVar(conf, ConfVars.AUTHORIZATION_STORAGE_AUTH_CHECKS);
  }

  private MetaStoreFS getMetaStoreFsHandler(Configuration conf)
      throws MetaException {
    String handlerClassStr = MetastoreConf.getVar(conf, ConfVars.FS_HANDLER_CLS);
    try {
      Class<? extends MetaStoreFS> handlerClass = (Class<? extends MetaStoreFS>) Class
          .forName(handlerClassStr, true, JavaUtils.getClassLoader());
      MetaStoreFS handler = ReflectionUtils.newInstance(handlerClass, conf);
      return handler;
    } catch (ClassNotFoundException e) {
      throw new MetaException("Error in loading MetaStoreFS handler."
          + e.getMessage());
    }
  }


  /**
   * Helper functions to convert IOException to MetaException
   */
  public static FileSystem getFs(Path f, Configuration conf) throws MetaException {
    try {
      return f.getFileSystem(conf);
    } catch (IOException e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return null;
  }

  public FileSystem getFs(Path f) throws MetaException {
    return getFs(f, conf);
  }


  /**
   * Hadoop File System reverse lookups paths with raw ip addresses The File
   * System URI always contains the canonical DNS name of the Namenode.
   * Subsequently, operations on paths with raw ip addresses cause an exception
   * since they don't match the file system URI.
   *
   * This routine solves this problem by replacing the scheme and authority of a
   * path with the scheme and authority of the FileSystem that it maps to.
   *
   * @param path
   *          Path to be canonicalized
   * @return Path with canonical scheme and authority
   */
  public static Path getDnsPath(Path path, Configuration conf) throws MetaException {
    FileSystem fs = getFs(path, conf);
    return (new Path(fs.getUri().getScheme(), fs.getUri().getAuthority(), path
        .toUri().getPath()));
  }

  public Path getDnsPath(Path path) throws MetaException {
    return getDnsPath(path, conf);
  }

  /**
   * Resolve the configured warehouse root dir with respect to the configuration
   * This involves opening the FileSystem corresponding to the warehouse root
   * dir (but that should be ok given that this is only called during DDL
   * statements for non-external tables).
   */
  public Path getWhRoot() throws MetaException {
    if (whRoot != null) {
      return whRoot;
    }
    whRoot = getDnsPath(new Path(whRootString));
    return whRoot;
  }

  public Path getDatabasePath(Database db) throws MetaException {
    if (db.getName().equalsIgnoreCase(DEFAULT_DATABASE_NAME)) {
      return getWhRoot();
    }
    return new Path(db.getLocationUri());
  }

  public Path getDefaultDatabasePath(String dbName) throws MetaException {
    if (dbName.equalsIgnoreCase(DEFAULT_DATABASE_NAME)) {
      return getWhRoot();
    }
    return new Path(getWhRoot(), dbName.toLowerCase() + DATABASE_WAREHOUSE_SUFFIX);
  }

  /**
   * Returns the default location of the table path using the parent database's location
   * @param db Database where the table is created
   * @param tableName table name
   * @return
   * @throws MetaException
   */
  public Path getDefaultTablePath(Database db, String tableName)
      throws MetaException {
    return getDnsPath(new Path(getDatabasePath(db), MetaStoreUtils.encodeTableName(tableName.toLowerCase())));
  }

  public static String getQualifiedName(Table table) {
    return getQualifiedName(table.getDbName(), table.getTableName());
  }

  public static String getQualifiedName(String dbName, String tableName) {
    return dbName + "." + tableName;
  }

  public static String getQualifiedName(Partition partition) {
    return partition.getDbName() + "." + partition.getTableName() + partition.getValues();
  }

  public boolean mkdirs(Path f) throws MetaException {
    FileSystem fs;
    try {
      fs = getFs(f);
      return FileUtils.mkdir(fs, f);
    } catch (IOException e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return false;
  }

  public boolean renameDir(Path sourcePath, Path destPath, boolean needCmRecycle) throws MetaException {
    try {
      if (needCmRecycle) {
        // Copy the source files to cmroot. As the client will move the source files to another
        // location, we should make a copy of the files to cmroot instead of moving it.
        cm.recycle(sourcePath, RecycleType.COPY, true);
      }
      FileSystem srcFs = getFs(sourcePath);
      FileSystem destFs = getFs(destPath);
      return FileUtils.rename(srcFs, destFs, sourcePath, destPath);
    } catch (Exception ex) {
      MetaStoreUtils.logAndThrowMetaException(ex);
    }
    return false;
  }

  void addToChangeManagement(Path file) throws MetaException {
    cm.recycle(file, RecycleType.COPY, true);
  }

  public boolean deleteDir(Path f, boolean recursive) throws MetaException {
    return deleteDir(f, recursive, false);
  }

  public boolean deleteDir(Path f, boolean recursive, boolean ifPurge) throws MetaException {
    cm.recycle(f, RecycleType.MOVE, ifPurge);
    FileSystem fs = getFs(f);
    return fsHandler.deleteDir(fs, f, recursive, ifPurge, conf);
  }

  public void recycleDirToCmPath(Path f, boolean ifPurge) throws MetaException {
    cm.recycle(f, RecycleType.MOVE, ifPurge);
    return;
  }

  public boolean isEmpty(Path path) throws IOException, MetaException {
    ContentSummary contents = getFs(path).getContentSummary(path);
    if (contents != null && contents.getFileCount() == 0 && contents.getDirectoryCount() == 1) {
      return true;
    }
    return false;
  }

  public boolean isWritable(Path path) throws IOException {
    if (!storageAuthCheck) {
      // no checks for non-secure hadoop installations
      return true;
    }
    if (path == null) { //what??!!
      return false;
    }
    final FileStatus stat;
    final FileSystem fs;
    try {
      fs = getFs(path);
      stat = fs.getFileStatus(path);
      HdfsUtils.checkFileAccess(fs, stat, FsAction.WRITE);
      return true;
    } catch (FileNotFoundException fnfe){
      // File named by path doesn't exist; nothing to validate.
      return true;
    } catch (Exception e) {
      // all other exceptions are considered as emanating from
      // unauthorized accesses
      if (LOG.isDebugEnabled()) {
        LOG.debug("Exception when checking if path (" + path + ")", e);
      }
      return false;
    }
  }

  private static String escapePathName(String path) {
    return FileUtils.escapePathName(path);
  }

  private static String unescapePathName(String path) {
    return FileUtils.unescapePathName(path);
  }

  /**
   * Given a partition specification, return the path corresponding to the
   * partition spec. By default, the specification does not include dynamic partitions.
   * @param spec
   * @return string representation of the partition specification.
   * @throws MetaException
   */
  public static String makePartPath(Map<String, String> spec)
      throws MetaException {
    return makePartName(spec, true);
  }

  /**
   * Makes a partition name from a specification
   * @param spec
   * @param addTrailingSeperator if true, adds a trailing separator e.g. 'ds=1/'
   * @return partition name
   * @throws MetaException
   */
  public static String makePartName(Map<String, String> spec,
      boolean addTrailingSeperator)
      throws MetaException {
    StringBuilder suffixBuf = new StringBuilder();
    int i = 0;
    for (Entry<String, String> e : spec.entrySet()) {
      if (e.getValue() == null || e.getValue().length() == 0) {
        throw new MetaException("Partition spec is incorrect. " + spec);
      }
      if (i>0) {
        suffixBuf.append(Path.SEPARATOR);
      }
      suffixBuf.append(escapePathName(e.getKey()));
      suffixBuf.append('=');
      suffixBuf.append(escapePathName(e.getValue()));
      i++;
    }
    if (addTrailingSeperator) {
      suffixBuf.append(Path.SEPARATOR);
    }
    return suffixBuf.toString();
  }
  /**
   * Given a dynamic partition specification, return the path corresponding to the
   * static part of partition specification. This is basically a copy of makePartName
   * but we get rid of MetaException since it is not serializable.
   * @param spec
   * @return string representation of the static part of the partition specification.
   */
  public static String makeDynamicPartName(Map<String, String> spec) {
    StringBuilder suffixBuf = new StringBuilder();
    for (Entry<String, String> e : spec.entrySet()) {
      if (e.getValue() != null && e.getValue().length() > 0) {
        suffixBuf.append(escapePathName(e.getKey()));
        suffixBuf.append('=');
        suffixBuf.append(escapePathName(e.getValue()));
        suffixBuf.append(Path.SEPARATOR);
      } else { // stop once we see a dynamic partition
        break;
      }
    }
    return suffixBuf.toString();
  }

  static final Pattern pat = Pattern.compile("([^/]+)=([^/]+)");

  private static final Pattern slash = Pattern.compile("/");

  /**
   * Extracts values from partition name without the column names.
   * @param name Partition name.
   * @param result The result. Must be pre-sized to the expected number of columns.
   */
  public static AbstractList<String> makeValsFromName(
      String name, AbstractList<String> result) throws MetaException {
    assert name != null;
    String[] parts = slash.split(name, 0);
    if (result == null) {
      result = new ArrayList<>(parts.length);
      for (int i = 0; i < parts.length; ++i) {
        result.add(null);
      }
    } else if (parts.length != result.size()) {
      throw new MetaException(
          "Expected " + result.size() + " components, got " + parts.length + " (" + name + ")");
    }
    for (int i = 0; i < parts.length; ++i) {
      int eq = parts[i].indexOf('=');
      if (eq <= 0) {
        throw new MetaException("Unexpected component " + parts[i]);
      }
      result.set(i, unescapePathName(parts[i].substring(eq + 1)));
    }
    return result;
  }

  public static LinkedHashMap<String, String> makeSpecFromName(String name)
      throws MetaException {
    if (name == null || name.isEmpty()) {
      throw new MetaException("Partition name is invalid. " + name);
    }
    LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();
    makeSpecFromName(partSpec, new Path(name), null);
    return partSpec;
  }

  public static boolean makeSpecFromName(Map<String, String> partSpec, Path currPath,
      Set<String> requiredKeys) {
    List<String[]> kvs = new ArrayList<>();
    do {
      String component = currPath.getName();
      Matcher m = pat.matcher(component);
      if (m.matches()) {
        String k = unescapePathName(m.group(1));
        String v = unescapePathName(m.group(2));
        String[] kv = new String[2];
        kv[0] = k;
        kv[1] = v;
        kvs.add(kv);
      }
      currPath = currPath.getParent();
    } while (currPath != null && !currPath.getName().isEmpty());

    // reverse the list since we checked the part from leaf dir to table's base dir
    for (int i = kvs.size(); i > 0; i--) {
      String key = kvs.get(i - 1)[0];
      if (requiredKeys != null) {
        requiredKeys.remove(key);
      }
      partSpec.put(key, kvs.get(i - 1)[1]);
    }
    if (requiredKeys == null || requiredKeys.isEmpty()) return true;
    LOG.warn("Cannot create partition spec from " + currPath + "; missing keys " + requiredKeys);
    return false;
  }

  public static Map<String, String> makeEscSpecFromName(String name) throws MetaException {

    if (name == null || name.isEmpty()) {
      throw new MetaException("Partition name is invalid. " + name);
    }
    LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();

    Path currPath = new Path(name);

    List<String[]> kvs = new ArrayList<>();
    do {
      String component = currPath.getName();
      Matcher m = pat.matcher(component);
      if (m.matches()) {
        String k = m.group(1);
        String v = m.group(2);
        String[] kv = new String[2];
        kv[0] = k;
        kv[1] = v;
        kvs.add(kv);
      }
      currPath = currPath.getParent();
    } while (currPath != null && !currPath.getName().isEmpty());

    // reverse the list since we checked the part from leaf dir to table's base dir
    for (int i = kvs.size(); i > 0; i--) {
      partSpec.put(kvs.get(i - 1)[0], kvs.get(i - 1)[1]);
    }

    return partSpec;
  }

  /**
   * Returns the default partition path of a table within a given database and partition key value
   * pairs. It uses the database location and appends it the table name and the partition key,value
   * pairs to create the Path for the partition directory
   *
   * @param db - parent database which is used to get the base location of the partition directory
   * @param tableName - table name for the partitions
   * @param pm - Partition key value pairs
   * @return
   * @throws MetaException
   */
  public Path getDefaultPartitionPath(Database db, String tableName,
      Map<String, String> pm) throws MetaException {
    return getPartitionPath(getDefaultTablePath(db, tableName), pm);
  }

  /**
   * Returns the path object for the given partition key-value pairs and the base location
   *
   * @param tblPath - the base location for the partitions. Typically the table location
   * @param pm - Partition key value pairs
   * @return
   * @throws MetaException
   */
  public Path getPartitionPath(Path tblPath, Map<String, String> pm)
      throws MetaException {
    return new Path(tblPath, makePartPath(pm));
  }

  /**
   * Given a database, a table and the partition key value pairs this method returns the Path object
   * corresponding to the partition key value pairs. It uses the table location if available else
   * uses the database location for constructing the path corresponding to the partition key-value
   * pairs
   *
   * @param db - Parent database of the given table
   * @param table - Table for which the partition key-values are given
   * @param vals - List of values for the partition keys
   * @return Path corresponding to the partition key-value pairs
   * @throws MetaException
   */
  public Path getPartitionPath(Database db, Table table, List<String> vals)
      throws MetaException {
    List<FieldSchema> partKeys = table.getPartitionKeys();
    if (partKeys == null || (partKeys.size() != vals.size())) {
      throw new MetaException("Invalid number of partition keys found for " + table.getTableName());
    }
    Map<String, String> pm = new LinkedHashMap<>(vals.size());
    int i = 0;
    for (FieldSchema key : partKeys) {
      pm.put(key.getName(), vals.get(i));
      i++;
    }

    if (table.getSd().getLocation() != null) {
      return getPartitionPath(getDnsPath(new Path(table.getSd().getLocation())), pm);
    } else {
      return getDefaultPartitionPath(db, table.getTableName(), pm);
    }
  }

  public boolean isDir(Path f) throws MetaException {
    FileSystem fs;
    try {
      fs = getFs(f);
      FileStatus fstatus = fs.getFileStatus(f);
      if (!fstatus.isDir()) {
        return false;
      }
    } catch (FileNotFoundException e) {
      return false;
    } catch (IOException e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return true;
  }

  public static String makePartName(List<FieldSchema> partCols,
      List<String> vals) throws MetaException {
    return makePartName(partCols, vals, null);
  }

  /**
   * @param desc
   * @return array of FileStatus objects corresponding to the files
   * making up the passed storage description
   */
  public FileStatus[] getFileStatusesForSD(StorageDescriptor desc)
      throws MetaException {
    return getFileStatusesForLocation(desc.getLocation());
  }

  /**
   * @param location
   * @return array of FileStatus objects corresponding to the files
   * making up the passed storage description
   */
  public FileStatus[] getFileStatusesForLocation(String location)
      throws MetaException {
    try {
      Path path = new Path(location);
      FileSystem fileSys = path.getFileSystem(conf);
      return FileUtils.getFileStatusRecurse(path, -1, fileSys);
    } catch (IOException ioe) {
      MetaStoreUtils.logAndThrowMetaException(ioe);
    }
    return null;
  }

  /**
   * @param db database
   * @param table table
   * @return array of FileStatus objects corresponding to the files making up the passed
   * unpartitioned table
   */
  public FileStatus[] getFileStatusesForUnpartitionedTable(Database db, Table table)
      throws MetaException {
    Path tablePath = getDnsPath(new Path(table.getSd().getLocation()));
    try {
      FileSystem fileSys = tablePath.getFileSystem(conf);
      return FileUtils.getFileStatusRecurse(tablePath, -1, fileSys);
    } catch (IOException ioe) {
      MetaStoreUtils.logAndThrowMetaException(ioe);
    }
    return null;
  }

  /**
   * Makes a valid partition name.
   * @param partCols The partition columns
   * @param vals The partition values
   * @param defaultStr
   *    The default name given to a partition value if the respective value is empty or null.
   * @return An escaped, valid partition name.
   * @throws MetaException
   */
  public static String makePartName(List<FieldSchema> partCols,
      List<String> vals, String defaultStr) throws MetaException {
    if ((partCols.size() != vals.size()) || (partCols.size() == 0)) {
      String errorStr = "Invalid partition key & values; keys [";
      for (FieldSchema fs : partCols) {
        errorStr += (fs.getName() + ", ");
      }
      errorStr += "], values [";
      for (String val : vals) {
        errorStr += (val + ", ");
      }
      throw new MetaException(errorStr + "]");
    }
    List<String> colNames = new ArrayList<>();
    for (FieldSchema col: partCols) {
      colNames.add(col.getName());
    }
    return FileUtils.makePartName(colNames, vals, defaultStr);
  }

  public static List<String> getPartValuesFromPartName(String partName)
      throws MetaException {
    LinkedHashMap<String, String> partSpec = Warehouse.makeSpecFromName(partName);
    List<String> values = new ArrayList<>();
    values.addAll(partSpec.values());
    return values;
  }

  public static Map<String, String> makeSpecFromValues(List<FieldSchema> partCols,
      List<String> values) {
    Map<String, String> spec = new LinkedHashMap<>();
    for (int i = 0; i < values.size(); i++) {
      spec.put(partCols.get(i).getName(), values.get(i));
    }
    return spec;
  }
}
