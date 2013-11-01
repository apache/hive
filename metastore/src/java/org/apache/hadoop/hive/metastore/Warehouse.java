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

import static org.apache.hadoop.hive.metastore.MetaStoreUtils.DATABASE_WAREHOUSE_SUFFIX;
import static org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.security.auth.login.LoginException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.HiveStatsUtils;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This class represents a warehouse where data of Hive tables is stored
 */
public class Warehouse {
  private Path whRoot;
  private final Configuration conf;
  private final String whRootString;

  public static final Log LOG = LogFactory.getLog("hive.metastore.warehouse");

  private MetaStoreFS fsHandler = null;
  private boolean storageAuthCheck = false;
  private boolean inheritPerms = false;

  public Warehouse(Configuration conf) throws MetaException {
    this.conf = conf;
    whRootString = HiveConf.getVar(conf, HiveConf.ConfVars.METASTOREWAREHOUSE);
    if (StringUtils.isBlank(whRootString)) {
      throw new MetaException(HiveConf.ConfVars.METASTOREWAREHOUSE.varname
          + " is not set in the config or blank");
    }
    fsHandler = getMetaStoreFsHandler(conf);
    storageAuthCheck = HiveConf.getBoolVar(conf,
        HiveConf.ConfVars.METASTORE_AUTHORIZATION_STORAGE_AUTH_CHECKS);
    inheritPerms = HiveConf.getBoolVar(conf,
        HiveConf.ConfVars.HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS);
  }

  private MetaStoreFS getMetaStoreFsHandler(Configuration conf)
      throws MetaException {
    String handlerClassStr = HiveConf.getVar(conf,
        HiveConf.ConfVars.HIVE_METASTORE_FS_HANDLER_CLS);
    try {
      Class<? extends MetaStoreFS> handlerClass = (Class<? extends MetaStoreFS>) Class
          .forName(handlerClassStr, true, JavaUtils.getClassLoader());
      MetaStoreFS handler = (MetaStoreFS) ReflectionUtils.newInstance(
          handlerClass, conf);
      return handler;
    } catch (ClassNotFoundException e) {
      throw new MetaException("Error in loading MetaStoreFS handler."
          + e.getMessage());
    }
  }


  /**
   * Helper functions to convert IOException to MetaException
   */
  public FileSystem getFs(Path f) throws MetaException {
    try {
      return f.getFileSystem(conf);
    } catch (IOException e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return null;
  }

  public static void closeFs(FileSystem fs) throws MetaException {
    try {
      if (fs != null) {
        fs.close();
      }
    } catch (IOException e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
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
  public Path getDnsPath(Path path) throws MetaException {
    FileSystem fs = getFs(path);
    return (new Path(fs.getUri().getScheme(), fs.getUri().getAuthority(), path
        .toUri().getPath()));
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

  public Path getTablePath(String whRootString, String tableName) throws MetaException {
    Path whRoot = getDnsPath(new Path(whRootString));
    return new Path(whRoot, tableName.toLowerCase());
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


  public Path getTablePath(Database db, String tableName)
      throws MetaException {
    return getDnsPath(new Path(getDatabasePath(db), tableName.toLowerCase()));
  }

  public boolean mkdirs(Path f) throws MetaException {
    FileSystem fs = null;
    try {
      fs = getFs(f);
      LOG.debug("Creating directory if it doesn't exist: " + f);
      //Check if the directory already exists. We want to change the permission
      //to that of the parent directory only for newly created directories.
      if (this.inheritPerms) {
        try {
          return fs.getFileStatus(f).isDir();
        } catch (FileNotFoundException ignore) {
        }
      }
      boolean success = fs.mkdirs(f);
      if (this.inheritPerms && success) {
        // Set the permission of parent directory.
        fs.setPermission(f, fs.getFileStatus(f.getParent()).getPermission());
      }
      return success;
    } catch (IOException e) {
      closeFs(fs);
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return false;
  }

  public boolean renameDir(Path sourcePath, Path destPath) throws MetaException {
    FileSystem fs = null;
    try {
      fs = getFs(sourcePath);
      fs.rename(sourcePath, destPath);
      return true;
    } catch (Exception ex) {
      MetaStoreUtils.logAndThrowMetaException(ex);
    }
    return false;
  }

  public boolean deleteDir(Path f, boolean recursive) throws MetaException {
    FileSystem fs = getFs(f);
    return fsHandler.deleteDir(fs, f, recursive, conf);
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
    try {
      stat = getFs(path).getFileStatus(path);
    } catch (FileNotFoundException fnfe){
      // File named by path doesn't exist; nothing to validate.
      return true;
    } catch (Exception e) {
      // all other exceptions are considered as emanating from
      // unauthorized accesses
      return false;
    }
    final UserGroupInformation ugi;
    try {
      ugi = ShimLoader.getHadoopShims().getUGIForConf(conf);
    } catch (LoginException le) {
      throw new IOException(le);
    }
    String user = ShimLoader.getHadoopShims().getShortUserName(ugi);
    //check whether owner can delete
    if (stat.getOwner().equals(user) &&
        stat.getPermission().getUserAction().implies(FsAction.WRITE)) {
      return true;
    }
    //check whether group of the user can delete
    if (stat.getPermission().getGroupAction().implies(FsAction.WRITE)) {
      String[] groups = ugi.getGroupNames();
      if (ArrayUtils.contains(groups, stat.getGroup())) {
        return true;
      }
    }
    //check whether others can delete (uncommon case!!)
    if (stat.getPermission().getOtherAction().implies(FsAction.WRITE)) {
      return true;
    }
    return false;
  }
  /*
  // NOTE: This is for generating the internal path name for partitions. Users
  // should always use the MetaStore API to get the path name for a partition.
  // Users should not directly take partition values and turn it into a path
  // name by themselves, because the logic below may change in the future.
  //
  // In the future, it's OK to add new chars to the escape list, and old data
  // won't be corrupt, because the full path name in metastore is stored.
  // In that case, Hive will continue to read the old data, but when it creates
  // new partitions, it will use new names.
  static BitSet charToEscape = new BitSet(128);
  static {
    for (char c = 0; c < ' '; c++) {
      charToEscape.set(c);
    }
    char[] clist = new char[] { '"', '#', '%', '\'', '*', '/', ':', '=', '?',
        '\\', '\u00FF' };
    for (char c : clist) {
      charToEscape.set(c);
    }
  }

  static boolean needsEscaping(char c) {
    return c >= 0 && c < charToEscape.size() && charToEscape.get(c);
  }
  */

  static String escapePathName(String path) {
    return FileUtils.escapePathName(path);
  }

  static String unescapePathName(String path) {
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
  public static void makeValsFromName(
      String name, AbstractList<String> result) throws MetaException {
    assert name != null;
    String[] parts = slash.split(name, 0);
    if (parts.length != result.size()) {
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
  }

  public static LinkedHashMap<String, String> makeSpecFromName(String name)
      throws MetaException {
    if (name == null || name.isEmpty()) {
      throw new MetaException("Partition name is invalid. " + name);
    }
    LinkedHashMap<String, String> partSpec = new LinkedHashMap<String, String>();
    makeSpecFromName(partSpec, new Path(name));
    return partSpec;
  }

  public static void makeSpecFromName(Map<String, String> partSpec, Path currPath) {
    List<String[]> kvs = new ArrayList<String[]>();
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
      partSpec.put(kvs.get(i - 1)[0], kvs.get(i - 1)[1]);
    }
  }

  public static Map<String, String> makeEscSpecFromName(String name) throws MetaException {

    if (name == null || name.isEmpty()) {
      throw new MetaException("Partition name is invalid. " + name);
    }
    LinkedHashMap<String, String> partSpec = new LinkedHashMap<String, String>();

    Path currPath = new Path(name);

    List<String[]> kvs = new ArrayList<String[]>();
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

  public Path getPartitionPath(Database db, String tableName,
      LinkedHashMap<String, String> pm) throws MetaException {
    return new Path(getTablePath(db, tableName), makePartPath(pm));
  }

  public Path getPartitionPath(Path tblPath, LinkedHashMap<String, String> pm)
      throws MetaException {
    return new Path(tblPath, makePartPath(pm));
  }

  public boolean isDir(Path f) throws MetaException {
    FileSystem fs = null;
    try {
      fs = getFs(f);
      FileStatus fstatus = fs.getFileStatus(f);
      if (!fstatus.isDir()) {
        return false;
      }
    } catch (FileNotFoundException e) {
      return false;
    } catch (IOException e) {
      closeFs(fs);
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return true;
  }

  public static String makePartName(List<FieldSchema> partCols,
      List<String> vals) throws MetaException {
    return makePartName(partCols, vals, null);
  }

  /**
   * @param partn
   * @return array of FileStatus objects corresponding to the files making up the passed partition
   */
  public FileStatus[] getFileStatusesForPartition(Partition partn)
      throws MetaException {
    try {
      Path path = new Path(partn.getSd().getLocation());
      FileSystem fileSys = path.getFileSystem(conf);
      /* consider sub-directory created from list bucketing. */
      int listBucketingDepth = calculateListBucketingDMLDepth(partn);
      return HiveStatsUtils.getFileStatusRecurse(path, (1 + listBucketingDepth), fileSys);
    } catch (IOException ioe) {
      MetaStoreUtils.logAndThrowMetaException(ioe);
    }
    return null;
  }

  /**
   * List bucketing will introduce sub-directories.
   * calculate it here in order to go to the leaf directory
   * so that we can count right number of files.
   * @param partn
   * @return
   */
  private static int calculateListBucketingDMLDepth(Partition partn) {
    // list bucketing will introduce more files
    int listBucketingDepth = 0;
    SkewedInfo skewedInfo = partn.getSd().getSkewedInfo();
    if ((skewedInfo != null) && (skewedInfo.getSkewedColNames() != null)
        && (skewedInfo.getSkewedColNames().size() > 0)
        && (skewedInfo.getSkewedColValues() != null)
        && (skewedInfo.getSkewedColValues().size() > 0)
        && (skewedInfo.getSkewedColValueLocationMaps() != null)
        && (skewedInfo.getSkewedColValueLocationMaps().size() > 0)) {
      listBucketingDepth = skewedInfo.getSkewedColNames().size();
    }
    return listBucketingDepth;
  }

  /**
   * @param table
   * @return array of FileStatus objects corresponding to the files making up the passed
   * unpartitioned table
   */
  public FileStatus[] getFileStatusesForUnpartitionedTable(Database db, Table table)
      throws MetaException {
    Path tablePath = getTablePath(db, table.getTableName());
    try {
      FileSystem fileSys = tablePath.getFileSystem(conf);
      return HiveStatsUtils.getFileStatusRecurse(tablePath, 1, fileSys);
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
      throw new MetaException("Invalid partition key & values");
    }
    List<String> colNames = new ArrayList<String>();
    for (FieldSchema col: partCols) {
      colNames.add(col.getName());
    }
    return FileUtils.makePartName(colNames, vals, defaultStr);
  }

  public static List<String> getPartValuesFromPartName(String partName)
      throws MetaException {
    LinkedHashMap<String, String> partSpec = Warehouse.makeSpecFromName(partName);
    List<String> values = new ArrayList<String>();
    values.addAll(partSpec.values());
    return values;
  }

}
