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

package org.apache.iceberg.mr.hive.actions;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.MetadataTableType.ALL_ENTRIES;

public class HiveIcebergDeleteOrphanFiles implements DeleteOrphanFiles {

  public static final String METADATA_FOLDER_NAME = "metadata";
  public static final String DATA_FOLDER_NAME = "data";
  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergDeleteOrphanFiles.class);
  private String tableLocation;
  private long olderThanTimestamp = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3);
  private Consumer<String> deleteFunc;
  private ExecutorService deleteExecutorService = MoreExecutors.newDirectExecutorService();

  private final Configuration conf;
  private final Table table;

  public HiveIcebergDeleteOrphanFiles(Configuration conf, Table table) {
    this.conf = conf;
    this.table = table;
    this.deleteFunc = file -> table.io().deleteFile(file);
    this.tableLocation = table.location();
  }

  @Override
  public HiveIcebergDeleteOrphanFiles location(String location) {
    this.tableLocation = location;
    return this;
  }

  @Override
  public HiveIcebergDeleteOrphanFiles olderThan(long newOlderThanTimestamp) {
    this.olderThanTimestamp = newOlderThanTimestamp;
    return this;
  }

  // TODO: Implement later, if there is any use case.
  @Override
  public HiveIcebergDeleteOrphanFiles deleteWith(Consumer<String> newDeleteFunc) {
    this.deleteFunc = newDeleteFunc;
    return this;
  }

  @Override
  public HiveIcebergDeleteOrphanFiles executeDeleteWith(ExecutorService executorService) {
    this.deleteExecutorService = executorService;
    return this;
  }

  @Override
  public Result execute() {
    LOG.info("Cleaning orphan files for {}", table.name());
    HiveIcebergDeleteOrphanFilesResult result = new HiveIcebergDeleteOrphanFilesResult();
    result.addDeletedFiles(cleanContentFiles(olderThanTimestamp));
    result.addDeletedFiles(cleanMetadata(olderThanTimestamp));

    LOG.debug("Deleting {} files while cleaning orphan files for {}", result.deletedFiles.size(), table.name());
    Tasks.foreach(result.deletedFiles).executeWith(deleteExecutorService).retry(3)
        .stopRetryOn(FileNotFoundException.class).suppressFailureWhenFinished().onFailure((file, thrown) ->
          LOG.warn("Delete failed for file: {}", file, thrown)).run(deleteFunc::accept);
    return result;
  }

  private Set<String> cleanContentFiles(long lastTime) {
    Set<String> validFiles = Sets.union(getAllContentFilePath(), getAllStatisticsFilePath(table));
    LOG.debug("Valid content file for {} are {}", table.name(), validFiles.size());
    try {
      Path dataPath = new Path(tableLocation, DATA_FOLDER_NAME);
      return getFilesToBeDeleted(lastTime, validFiles, dataPath);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  public Set<String> getAllContentFilePath() {
    Set<String> validFilesPath = Sets.newHashSet();
    Table metadatTable = getMetadataTable();

    TableScan tableScan = metadatTable.newScan();
    CloseableIterable<FileScanTask> manifestFileScanTasks = tableScan.planFiles();
    CloseableIterable<StructLike> entries = CloseableIterable.concat(entriesOfManifest(manifestFileScanTasks));

    for (StructLike entry : entries) {
      StructLike fileRecord = entry.get(4, StructLike.class);
      String filePath = fileRecord.get(1, String.class);
      validFilesPath.add(getUriPath(filePath));
    }
    return validFilesPath;
  }

  private Iterable<CloseableIterable<StructLike>> entriesOfManifest(
      CloseableIterable<FileScanTask> fileScanTasks) {
    return Iterables.transform(
        fileScanTasks,
        task -> {
          assert task != null;
          return ((DataTask) task).rows();
        });
  }

  public static Set<String> getAllStatisticsFilePath(Table table) {
    return ReachableFileUtil.statisticsFilesLocations(table).stream().map(HiveIcebergDeleteOrphanFiles::getUriPath)
        .collect(Collectors.toSet());
  }

  protected Set<String> cleanMetadata(long lastTime) {
    LOG.info("{} start clean metadata files", table.name());
    try {
      Set<String> validFiles = getValidMetadataFiles(table);
      LOG.debug("Valid metadata files for {} are {}", table.name(), validFiles);
      Path metadataLocation = new Path(tableLocation, METADATA_FOLDER_NAME);
      return getFilesToBeDeleted(lastTime, validFiles, metadataLocation);
    } catch (IOException ioe) {
      throw new UncheckedIOException(ioe);
    }
  }

  private Set<String> getFilesToBeDeleted(long lastTime, Set<String> validFiles, Path location)
      throws IOException {
    Set<String> filesToDelete = Sets.newHashSet();
    FileSystem fs = location.getFileSystem(conf);
    RemoteIterator<LocatedFileStatus> metadataLocations = fs.listFiles(location, true);
    while (metadataLocations.hasNext()) {
      LocatedFileStatus metadataFile = metadataLocations.next();
      if (metadataFile.getModificationTime() < lastTime && !validFiles.contains(
          getUriPath(metadataFile.getPath().toString()))) {
        filesToDelete.add(metadataFile.getPath().toString());
      }
    }
    return filesToDelete;
  }

  private Table getMetadataTable() {
    return MetadataTableUtils.createMetadataTableInstance(((HasTableOperations) table).operations(), table.name(),
        table.name() + "#" + ALL_ENTRIES.name(), ALL_ENTRIES);
  }

  private static Set<String> getValidMetadataFiles(Table icebergTable) {
    Set<String> validFiles = Sets.newHashSet();
    Iterable<Snapshot> snapshots = icebergTable.snapshots();
    for (Snapshot snapshot : snapshots) {
      String manifestListLocation = snapshot.manifestListLocation();
      validFiles.add(getUriPath(manifestListLocation));

      List<ManifestFile> manifestFiles = snapshot.allManifests(icebergTable.io());
      for (ManifestFile manifestFile : manifestFiles) {
        validFiles.add(getUriPath(manifestFile.path()));
      }
    }
    Stream.of(
            ReachableFileUtil.metadataFileLocations(icebergTable, false).stream(),
            ReachableFileUtil.statisticsFilesLocations(icebergTable).stream(),
            Stream.of(ReachableFileUtil.versionHintLocation(icebergTable)))
        .reduce(Stream::concat)
        .orElse(Stream.empty())
        .map(HiveIcebergDeleteOrphanFiles::getUriPath)
        .forEach(validFiles::add);

    return validFiles;
  }

  private static String getUriPath(String path) {
    return URI.create(path).getPath();
  }

  static class HiveIcebergDeleteOrphanFilesResult implements Result {

    private final Set<String> deletedFiles = Sets.newHashSet();

    @Override
    public Iterable<String> orphanFileLocations() {
      return deletedFiles;
    }

    public void addDeletedFiles(Set<String> files) {
      this.deletedFiles.addAll(files);
    }
  }
}
