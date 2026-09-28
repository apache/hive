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
import org.apache.hadoop.hive.metastore.utils.FileUtils;
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
  private final Path location;

  ScopedDeleteFileIO(FileIO delegate, String location) {
    this.delegate = delegate;
    this.location = new Path(location);
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
    var candidate = new Path(path);
    if (!FileUtils.isPathWithinSubtree(qualify(candidate, location), qualify(location, candidate))) {
      LOG.debug("Skipping delete outside table location {}: {}", location, path);
      return;
    }
    delegate.deleteFile(path);
  }

  /**
   * A path with no scheme refers to the same filesystem location as an otherwise-identical path
   * that does carry one, whichever side that happens to be: manifests can hold data file paths
   * written without a scheme, and a table's own location can equally be scheme-less (e.g. when a
   * caller supplies one explicitly). {@link org.apache.hadoop.fs.Path#equals} is scheme-sensitive,
   * so without this a schemeless path would never match its scheme-qualified counterpart in
   * {@link FileUtils#isPathWithinSubtree} even when it is nested directly under it. When both (or
   * neither) sides carry a scheme, {@code path} is returned unchanged, so a genuine mismatch (e.g.
   * a different scheme or authority) is still correctly treated as outside the table location.
   */
  private static Path qualify(Path path, Path other) {
    var pathUri = path.toUri();
    var otherUri = other.toUri();
    if (pathUri.getScheme() == null && otherUri.getScheme() != null) {
      return new Path(otherUri.getScheme(), otherUri.getAuthority(), pathUri.getPath());
    }
    return path;
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
}
