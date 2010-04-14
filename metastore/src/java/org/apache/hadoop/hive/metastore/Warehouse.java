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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;

/**
 * This class represents a warehouse where data of Hive tables is stored
 */
public class Warehouse {
  private Path whRoot;
  private final Configuration conf;
  String whRootString;

  public static final Log LOG = LogFactory.getLog("hive.metastore.warehouse");

  public Warehouse(Configuration conf) throws MetaException {
    this.conf = conf;
    whRootString = HiveConf.getVar(conf, HiveConf.ConfVars.METASTOREWAREHOUSE);
    if (StringUtils.isBlank(whRootString)) {
      throw new MetaException(HiveConf.ConfVars.METASTOREWAREHOUSE.varname
          + " is not set in the config or blank");
    }
  }

  /**
   * Helper function to convert IOException to MetaException
   */
  public FileSystem getFs(Path f) throws MetaException {
    try {
      return f.getFileSystem(conf);
    } catch (IOException e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return null;
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
  private Path getWhRoot() throws MetaException {
    if (whRoot != null) {
      return whRoot;
    }
    whRoot = getDnsPath(new Path(whRootString));
    return whRoot;
  }

  public Path getDefaultDatabasePath(String dbName) throws MetaException {
    if (dbName.equalsIgnoreCase(MetaStoreUtils.DEFAULT_DATABASE_NAME)) {
      return getWhRoot();
    }
    return new Path(getWhRoot(), dbName.toLowerCase() + ".db");
  }

  public Path getDefaultTablePath(String dbName, String tableName)
      throws MetaException {
    return new Path(getDefaultDatabasePath(dbName), tableName.toLowerCase());
  }

  public boolean mkdirs(Path f) throws MetaException {
    try {
      FileSystem fs = getFs(f);
      LOG.debug("Creating directory if it doesn't exist: " + f);
      return (fs.mkdirs(f) || fs.getFileStatus(f).isDir());
    } catch (IOException e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return false;
  }

  public boolean deleteDir(Path f, boolean recursive) throws MetaException {
    LOG.info("deleting  " + f);
    try {
      FileSystem fs = getFs(f);
      if (!fs.exists(f)) {
        return false;
      }

      // older versions of Hadoop don't have a Trash constructor based on the
      // Path or FileSystem. So need to achieve this by creating a dummy conf.
      // this needs to be filtered out based on version
      Configuration dupConf = new Configuration(conf);
      FileSystem.setDefaultUri(dupConf, fs.getUri());

      Trash trashTmp = new Trash(dupConf);
      if (trashTmp.moveToTrash(f)) {
        LOG.info("Moved to trash: " + f);
        return true;
      }
      if (fs.delete(f, true)) {
        LOG.info("Deleted the diretory " + f);
        return true;
      }
      if (fs.exists(f)) {
        throw new MetaException("Unable to delete directory: " + f);
      }
    } catch (FileNotFoundException e) {
      return true; // ok even if there is not data
    } catch (IOException e) {
      MetaStoreUtils.logAndThrowMetaException(e);
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
  public static String makePartName(Map<String, String> spec)
      throws MetaException {
    StringBuilder suffixBuf = new StringBuilder();
    for (Entry<String, String> e : spec.entrySet()) {
      if (e.getValue() == null || e.getValue().length() == 0) {
        throw new MetaException("Partition spec is incorrect. " + spec);
      }
      suffixBuf.append(escapePathName(e.getKey()));
      suffixBuf.append('=');
      suffixBuf.append(escapePathName(e.getValue()));
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

  public Path getPartitionPath(String dbName, String tableName,
      LinkedHashMap<String, String> pm) throws MetaException {
    return new Path(getDefaultTablePath(dbName, tableName), makePartName(pm));
  }

  public Path getPartitionPath(Path tblPath, LinkedHashMap<String, String> pm)
      throws MetaException {
    return new Path(tblPath, makePartName(pm));
  }

  public boolean isDir(Path f) throws MetaException {
    try {
      FileSystem fs = getFs(f);
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
    if ((partCols.size() != vals.size()) || (partCols.size() == 0)) {
      throw new MetaException("Invalid partition key & values");
    }
    List<String> colNames = new ArrayList<String>();
    for (FieldSchema col: partCols) {
      colNames.add(col.getName());
    }
    return FileUtils.makePartName(colNames, vals);
  }
}
