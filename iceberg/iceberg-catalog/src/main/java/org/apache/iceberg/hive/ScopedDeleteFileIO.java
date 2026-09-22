/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.hive;

import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link FileIO} decorator used by {@link HiveCatalog#dropTable(org.apache.iceberg.catalog.TableIdentifier,
 * boolean)} to fence purge deletions to files under a table's own location, regardless of what a table's
 * metadata or manifests actually reference.
 *
 * <p>This does not implement {@link org.apache.iceberg.io.SupportsBulkOperations} or
 * {@link org.apache.iceberg.io.SupportsPrefixOperations} even when the delegate does, so that
 * {@code CatalogUtil.dropTableData} is forced to route every deletion through {@link #deleteFile(String)}.
 */
class ScopedDeleteFileIO implements FileIO {
  private static final Logger LOG = LoggerFactory.getLogger(ScopedDeleteFileIO.class);

  private final FileIO delegate;
  private final String location;

  ScopedDeleteFileIO(FileIO delegate, String location) {
    this.delegate = delegate;
    this.location = normalize(location);
  }

  @Override
  public InputFile newInputFile(String path) {
    return delegate.newInputFile(path);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return delegate.newOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    if (!isContained(location, normalize(path))) {
      LOG.warn("Skipping delete outside table location {}: {}", location, path);
      return;
    }
    delegate.deleteFile(path);
  }

  @Override
  public Map<String, String> properties() {
    return delegate.properties();
  }

  @Override
  public void initialize(Map<String, String> properties) {
    delegate.initialize(properties);
  }

  @Override
  public void close() {
    delegate.close();
  }

  private static boolean isContained(String root, String candidate) {
    return candidate.equals(root) || candidate.startsWith(root.endsWith("/") ? root : root + "/");
  }

  private static String normalize(String location) {
    return new Path(location).toUri().normalize().toString();
  }
}
