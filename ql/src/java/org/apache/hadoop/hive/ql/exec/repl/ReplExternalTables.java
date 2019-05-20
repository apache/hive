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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.PathBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Format of the file used to dump information about external tables:
 * <p>
 * table_name1,[base64Encoded(table_dir_location)]\n
 *
 * The file generated here is explicitly used for data copy of external tables and hence handling of
 * writing this file is different than regular event handling for replication based on the conditions
 * specified in {@link org.apache.hadoop.hive.ql.parse.repl.dump.Utils#shouldReplicate}
 */
public final class ReplExternalTables {
  private static final Logger LOG = LoggerFactory.getLogger(ReplExternalTables.class);
  private static final String FIELD_SEPARATOR = ",";
  public static final String FILE_NAME = "_external_tables_info";
  private static final int MAX_RETRIES = 5;

  private ReplExternalTables(){}

  public static String externalTableLocation(HiveConf hiveConf, String location) throws SemanticException {
    String baseDir = hiveConf.get(HiveConf.ConfVars.REPL_EXTERNAL_TABLE_BASE_DIR.varname);
    Path basePath = new Path(baseDir);
    Path currentPath = new Path(location);
    Path dataLocation = externalTableDataPath(hiveConf, basePath, currentPath);

    LOG.info("Incoming external table location: {} , new location: {}", location, dataLocation.toString());
    return dataLocation.toString();
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
      throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(), e);
    }
    return dataPath;
  }

  public static class Writer implements Closeable {
    private static Logger LOG = LoggerFactory.getLogger(Writer.class);
    private final HiveConf hiveConf;
    private final Path writePath;
    private final boolean includeExternalTables;
    private final boolean dumpMetadataOnly;
    private OutputStream writer;

    Writer(Path dbRoot, HiveConf hiveConf) throws IOException {
      this.hiveConf = hiveConf;
      writePath = new Path(dbRoot, FILE_NAME);
      includeExternalTables = hiveConf.getBoolVar(HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES);
      dumpMetadataOnly = hiveConf.getBoolVar(HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY);
      if (shouldWrite()) {
        this.writer = FileSystem.get(hiveConf).create(writePath);
      }
    }

    private boolean shouldWrite() {
      return !dumpMetadataOnly && includeExternalTables;
    }

    /**
     * this will dump a single line per external table. it can include additional lines for the same
     * table if the table is partitioned and the partition location is outside the table.
     */
    void dataLocationDump(Table table)
        throws InterruptedException, IOException, HiveException {
      if (!shouldWrite()) {
        return;
      }
      if (!TableType.EXTERNAL_TABLE.equals(table.getTableType())) {
        throw new IllegalArgumentException(
            "only External tables can be writen via this writer, provided table is " + table
                .getTableType());
      }
      Path fullyQualifiedDataLocation =
          PathBuilder.fullyQualifiedHDFSUri(table.getDataLocation(), FileSystem.get(hiveConf));
      write(lineFor(table.getTableName(), fullyQualifiedDataLocation, hiveConf));
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
            fullyQualifiedDataLocation = PathBuilder
                .fullyQualifiedHDFSUri(partition.getDataLocation(), FileSystem.get(hiveConf));
            write(lineFor(table.getTableName(), fullyQualifiedDataLocation, hiveConf));
          }
        }
      }
    }

    private static String lineFor(String tableName, Path dataLoc, HiveConf hiveConf)
        throws IOException, SemanticException {
      StringWriter lineToWrite = new StringWriter();
      lineToWrite.append(tableName).append(FIELD_SEPARATOR);
      Path dataLocation =
          PathBuilder.fullyQualifiedHDFSUri(dataLoc, dataLoc.getFileSystem(hiveConf));
      byte[] encodedBytes = Base64.getEncoder()
          .encode(dataLocation.toString().getBytes(StandardCharsets.UTF_8));
      String encodedPath = new String(encodedBytes, StandardCharsets.UTF_8);
      lineToWrite.append(encodedPath).append("\n");
      return lineToWrite.toString();
    }

    private void write(String line) throws InterruptedException {
      int currentRetry = 0;
      while (currentRetry < MAX_RETRIES) {
        try {
          writer.write(line.getBytes(StandardCharsets.UTF_8));
          break;
        } catch (IOException e) {
          currentRetry++;
          if (currentRetry < MAX_RETRIES) {
            LOG.warn("failed to write data with maxRetries {} due to", currentRetry, e);
          } else {
            LOG.error("failed to write data with maxRetries {} due to", currentRetry, e);
            throw new RuntimeException("failed to write data", e);
          }
          Thread.sleep(100 * currentRetry * currentRetry);
          writer = openWriterAppendMode();
        }
      }
    }

    private OutputStream openWriterAppendMode() {
      try {
        // not sure if the exception was due to a incorrect state within the writer hence closing it
        close();
        return FileSystem.get(hiveConf).append(writePath);
      } catch (IOException e1) {
        String message = "there was an error to open the file {} in append mode";
        LOG.error(message, writePath.toString(), e1);
        throw new IllegalStateException(message, e1);
      }
    }

    @Override
    public void close() throws IOException {
      if (writer != null) {
        writer.close();
      }
    }
  }

  public static class Reader {
    private static Logger LOG = LoggerFactory.getLogger(Reader.class);
    private final HiveConf hiveConf;
    private final Path rootReplLoadPath;
    private final boolean isIncrementalPhase;

    public Reader(HiveConf conf, Path rootReplLoadPath, boolean isIncrementalPhase) {
      this.hiveConf = conf;
      this.rootReplLoadPath = rootReplLoadPath;
      this.isIncrementalPhase = isIncrementalPhase;
    }

    /**
     * currently we only support dump/load of single db and the db Dump location cannot be inferred from
     * the incoming dbNameOfPattern value since the load db name can be different from the target db Name
     * hence traverse 1 level down from rootReplLoadPath to look for the file providing the hdfs locations.
     */
    public Set<String> sourceLocationsToCopy() throws IOException {
      if (isIncrementalPhase) {
        return sourceLocationsToCopy(new Path(rootReplLoadPath, FILE_NAME));
      }

      // this is bootstrap load path
      Set<String> locationsToCopy = new HashSet<>();
      FileSystem fileSystem = rootReplLoadPath.getFileSystem(hiveConf);
      FileStatus[] fileStatuses = fileSystem.listStatus(rootReplLoadPath);
      for (FileStatus next : fileStatuses) {
        if (next.isDirectory()) {
          Path externalTableInfoPath = new Path(next.getPath(), FILE_NAME);
          locationsToCopy.addAll(sourceLocationsToCopy(externalTableInfoPath));
        }
      }
      return locationsToCopy;
    }

    private BufferedReader reader(FileSystem fs, Path externalTableInfo) throws IOException {
      InputStreamReader in = new InputStreamReader(fs.open(externalTableInfo), StandardCharsets.UTF_8);
      return new BufferedReader(in);
    }

    /**
     * The SET of source locations should never be created based on the table Name in
     * {@link #FILE_NAME} since there can be multiple entries for the same table in case the table is
     * partitioned and the partitions are added by providing a separate Location for that partition,
     * different than the table location.
     */
    private Set<String> sourceLocationsToCopy(Path externalTableInfo) throws IOException {
      Set<String> locationsToCopy = new HashSet<>();
      FileSystem fileSystem = externalTableInfo.getFileSystem(hiveConf);
      if (!fileSystem.exists(externalTableInfo)) {
        return locationsToCopy;
      }

      int currentRetry = 0;
      BufferedReader reader = null;
      while (currentRetry < MAX_RETRIES) {
        try {
          reader = reader(fileSystem, externalTableInfo);
          for (String line = reader.readLine(); line != null; line = reader.readLine()) {
            String[] splits = line.split(FIELD_SEPARATOR);
            locationsToCopy
                .add(new String(Base64.getDecoder().decode(splits[1]), StandardCharsets.UTF_8));
          }
          return locationsToCopy;
        } catch (IOException e) {
          currentRetry++;
          if (currentRetry < MAX_RETRIES) {
            closeQuietly(reader);
            LOG.warn("failed to read {}", externalTableInfo.toString(), e);
          } else {
            LOG.error("failed to read {}", externalTableInfo.toString(), e);
            throw e;
          }
        } finally {
          closeQuietly(reader);
        }
      }
      // we should never reach here
      throw new IllegalStateException("we should never reach this condition");
    }

    private static void closeQuietly(BufferedReader reader) {
      try {
        if (reader != null) {
          reader.close();
        }
      } catch (IOException e) {
        LOG.debug("error while closing reader ", e);
      }
    }
  }
}
