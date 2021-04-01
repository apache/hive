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
package org.apache.hadoop.hive.ql.exec.repl;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.repl.util.FileList;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.PathBuilder;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public class ReplExternalTables {
  private static final Logger LOG = LoggerFactory.getLogger(ReplExternalTables.class);
  private HiveConf hiveConf;
  private boolean includeExternalTables;
  private boolean dumpMetadataOnly;

  public ReplExternalTables(HiveConf conf) {
    this.hiveConf = conf;
    this.includeExternalTables = hiveConf.getBoolVar(HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES);
    this.dumpMetadataOnly = hiveConf.getBoolVar(HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY) ||
            hiveConf.getBoolVar(HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY_FOR_EXTERNAL_TABLE);
  }

  private boolean shouldWrite() {
    return !dumpMetadataOnly && includeExternalTables;
  }

  /**
   * this will dump a single line per external table. it can include additional lines for the same
   * table if the table is partitioned and the partition location is outside the table.
   * It returns list of all the external table locations.
   */
  void dataLocationDump(Table table, FileList fileList,
                        Path dbLoc, boolean isTableLevelReplication, HiveConf conf)
          throws InterruptedException, IOException, HiveException {
    if (!shouldWrite()) {
      return;
    }
    if (!TableType.EXTERNAL_TABLE.equals(table.getTableType())) {
      throw new IllegalArgumentException(
              "only External tables can be writen via this writer, provided table is " + table
                      .getTableType());
    }
    Path fullyQualifiedDataLocation = PathBuilder.fullyQualifiedHDFSUri(table.getDataLocation(), FileSystem.get(hiveConf));
    if (isTableLevelReplication || !FileUtils
            .isPathWithinSubtree(table.getDataLocation(), dbLoc)) {
      dirLocationToCopy(table.getTableName(), fileList, fullyQualifiedDataLocation, conf);
    }
    if (table.isPartitioned()) {
      List<Partition> partitions;
      try {
        partitions = Hive.get(hiveConf).getPartitions(table);
      } catch (HiveException e) {
        if (e.getCause() instanceof NoSuchObjectException) {
          // If table is dropped when dump in progress, just skip partitions data location dump
          LOG.debug(e.getMessage());
          return;
        }
        throw e;
      }

      for (Partition partition : partitions) {
        boolean partitionLocOutsideTableLoc = !FileUtils.isPathWithinSubtree(
                partition.getDataLocation(), table.getDataLocation()
        );
        if (partitionLocOutsideTableLoc) {
          // check if the entire db location is getting copied, then the
          // partition isn't inside the db location, if so we can skip
          // copying this separately.
          if (!isTableLevelReplication && FileUtils
                  .isPathWithinSubtree(partition.getDataLocation(), dbLoc)) {
            partitionLocOutsideTableLoc = false;
          }
        }
        if (partitionLocOutsideTableLoc) {
          fullyQualifiedDataLocation = PathBuilder
                  .fullyQualifiedHDFSUri(partition.getDataLocation(), FileSystem.get(hiveConf));
          dirLocationToCopy(table.getTableName(), fileList, fullyQualifiedDataLocation, conf);
        }
      }
    }
  }

  void dbLocationDump(String dbName, Path dbLocation, FileList fileList,
                      HiveConf conf) throws Exception {
    Path fullyQualifiedDataLocation = PathBuilder
            .fullyQualifiedHDFSUri(dbLocation, FileSystem.get(hiveConf));
    dirLocationToCopy(dbName, fileList, fullyQualifiedDataLocation, conf);
  }

  private void dirLocationToCopy(String tableName, FileList fileList, Path sourcePath, HiveConf conf)
          throws HiveException, IOException {
    Path basePath = getExternalTableBaseDir(conf);
    Path targetPath = externalTableDataPath(conf, basePath, sourcePath);
    //Here, when src and target are HA clusters with same NS, then sourcePath would have the correct host
    //whereas the targetPath would have an host that refers to the target cluster. This is fine for
    //data-copy running during dump as the correct logical locations would be used. But if data-copy runs during
    //load, then the remote location needs to point to the src cluster from where the data would be copied and
    //the common original NS would suffice for targetPath.
    if (hiveConf.getBoolVar(HiveConf.ConfVars.REPL_HA_DATAPATH_REPLACE_REMOTE_NAMESERVICE) &&
            hiveConf.getBoolVar(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET)) {
      String remoteNS = hiveConf.get(HiveConf.ConfVars.REPL_HA_DATAPATH_REPLACE_REMOTE_NAMESERVICE_NAME.varname);
      if (StringUtils.isEmpty(remoteNS)) {
        throw new SemanticException(ErrorMsg.REPL_INVALID_CONFIG_FOR_SERVICE
                .format("Configuration 'hive.repl.ha.datapath.replace.remote.nameservice.name' is not valid "
                        + remoteNS == null ? "null" : remoteNS, ReplUtils.REPL_HIVE_SERVICE));
      }
      targetPath = new Path(Utils.replaceHost(targetPath.toString(), sourcePath.toUri().getHost()));
      sourcePath = new Path(Utils.replaceHost(sourcePath.toString(), remoteNS));
    }
    fileList.add(new DirCopyWork(tableName, sourcePath, targetPath).convertToString());
  }

  public static String externalTableLocation(HiveConf hiveConf, String location) throws SemanticException {
    Path basePath = getExternalTableBaseDir(hiveConf);
    Path currentPath = new Path(location);
    Path dataLocation = externalTableDataPath(hiveConf, basePath, currentPath);

    LOG.info("Incoming external table location: {} , new location: {}", location, dataLocation.toString());
    return dataLocation.toString();
  }

  public static Path getExternalTableBaseDir(HiveConf hiveConf) throws SemanticException {
    String baseDir = hiveConf.get(HiveConf.ConfVars.REPL_EXTERNAL_TABLE_BASE_DIR.varname);
    URI baseDirUri  = StringUtils.isEmpty(baseDir) ? null : new Path(baseDir).toUri();
    if (baseDirUri == null || baseDirUri.getScheme() == null || baseDirUri.getAuthority() == null) {
      throw new SemanticException(ErrorMsg.REPL_INVALID_CONFIG_FOR_SERVICE.format(
              String.format("Fully qualified path for 'hive.repl.replica.external.table.base.dir' is required %s",
                      baseDir == null ? "" : "- ('" + baseDir + "')"), ReplUtils.REPL_HIVE_SERVICE));
    }
    return new Path(baseDirUri);
  }

  public static Path externalTableDataPath(HiveConf hiveConf, Path basePath, Path sourcePath)
          throws SemanticException {
    String baseUriPath = basePath.toUri().getPath();
    String sourceUriPath = sourcePath.toUri().getPath();

    // "/" is input for base directory, then we should use exact same path as source or else append
    // source path under the base directory.
    String targetPathWithoutSchemeAndAuth
            = "/".equalsIgnoreCase(baseUriPath) ? sourceUriPath : (baseUriPath + sourceUriPath);
    Path dataPath;
    try {
      dataPath = PathBuilder.fullyQualifiedHDFSUri(
              new Path(targetPathWithoutSchemeAndAuth),
              basePath.getFileSystem(hiveConf)
      );
    } catch (IOException e) {
      throw new SemanticException(ErrorMsg.REPL_INVALID_CONFIG_FOR_SERVICE.format(
        ErrorMsg.INVALID_PATH.getMsg(), ReplUtils.REPL_HIVE_SERVICE), e);
    }
    return dataPath;
  }
}
