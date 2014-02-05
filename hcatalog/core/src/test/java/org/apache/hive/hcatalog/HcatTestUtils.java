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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for tests
 */
public class HcatTestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HcatTestUtils.class);

  public static FsPermission perm007 = FsPermission.createImmutable((short) 0007); // -------rwx
  public static FsPermission perm070 = FsPermission.createImmutable((short) 0070); // ----rwx---
  public static FsPermission perm700 = FsPermission.createImmutable((short) 0700); // -rwx------
  public static FsPermission perm755 = FsPermission.createImmutable((short) 0755); // -rwxr-xr-x
  public static FsPermission perm777 = FsPermission.createImmutable((short) 0777); // -rwxrwxrwx
  public static FsPermission perm300 = FsPermission.createImmutable((short) 0300); // --wx------
  public static FsPermission perm500 = FsPermission.createImmutable((short) 0500); // -r-x------
  public static FsPermission perm555 = FsPermission.createImmutable((short) 0555); // -r-xr-xr-x

  /**
   * Returns the database path.
   */
  public static Path getDbPath(Hive hive, Warehouse wh, String dbName) throws MetaException, HiveException {
    return wh.getDatabasePath(hive.getDatabase(dbName));
  }

  /**
   * Removes all databases and tables from the metastore
   */
  public static void cleanupHMS(Hive hive, Warehouse wh, FsPermission defaultPerm)
    throws HiveException, MetaException, NoSuchObjectException {
    for (String dbName : hive.getAllDatabases()) {
      if (dbName.equals("default")) {
        continue;
      }
      try {
        Path path = getDbPath(hive, wh, dbName);
        FileSystem whFs = path.getFileSystem(hive.getConf());
        whFs.setPermission(path, defaultPerm);
      } catch (IOException ex) {
        //ignore
      }
      hive.dropDatabase(dbName, true, true, true);
    }

    //clean tables in default db
    for (String tablename : hive.getAllTables("default")) {
      hive.dropTable("default", tablename, true, true);
    }
  }

  public static void createTestDataFile(String filename, String[] lines) throws IOException {
    FileWriter writer = null;
    try {
      File file = new File(filename);
      file.deleteOnExit();
      writer = new FileWriter(file);
      for (String line : lines) {
        writer.write(line + "\n");
      }
    } finally {
      if (writer != null) {
        writer.close();
      }
    }

  }
}
