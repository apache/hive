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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.ContentCache;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The FileIO of a Hive catalog that caches the table metadata it reads ({@code io.manifest.cache-enabled}): metadata
 * JSON, manifest lists, manifests and statistics files, once per process and by location, since none of them is ever
 * rewritten in place. Iceberg keeps its manifest cache per FileIO instance, while Hive builds a catalog, and with it a
 * FileIO, for every table it loads, and deserializes a table, its FileIO included, for every query it plans: what one
 * query read was gone for the next. Only files in a table's metadata directory are cached; data and delete files
 * never are.
 */
public class MetadataCachingFileIO extends HadoopFileIO {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataCachingFileIO.class);
  private static final Map<List<Long>, ContentCache> CACHES = new ConcurrentHashMap<>();
  static final long DEFAULT_EXPIRATION_MS = TimeUnit.HOURS.toMillis(1);

  private static final String METADATA_DIR = "/metadata/";
  // a Hadoop catalog names its versions v1, v2, ... and starts over when a table is created again at the location
  private static final Pattern HADOOP_CATALOG_VERSION = Pattern.compile("v\\d+(\\.gz)?\\.metadata\\.json");

  private transient volatile ContentCache cache;

  MetadataCachingFileIO(Configuration conf) {
    super(conf);
  }

  /** Without the cache flag, which would have Iceberg cache the manifests again, per instance of this class. */
  @Override
  public void initialize(Map<String, String> properties) {
    super.initialize(Maps.filterKeys(properties, key -> !CatalogProperties.IO_MANIFEST_CACHE_ENABLED.equals(key)));
  }

  @Override
  public InputFile newInputFile(String path) {
    InputFile file = super.newInputFile(path);
    return isTableMetadata(path) ? cache().tryCache(file) : file;
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    InputFile file = super.newInputFile(path, length);
    return isTableMetadata(path) ? cache().tryCache(file) : file;
  }

  /** Every file a data file is opened through passes here, so the check is a few comparisons until it matches. */
  static boolean isTableMetadata(String location) {
    int name = location.lastIndexOf('/') + 1;
    if (!location.startsWith(METADATA_DIR, name - METADATA_DIR.length())) {
      return false;
    }
    return location.endsWith(".avro") || location.endsWith(".puffin") || location.endsWith(".stats") ||
        location.startsWith("partition-stats-", name) ||
        location.endsWith(".metadata.json") &&
            !HADOOP_CATALOG_VERSION.matcher(location).region(name, location.length()).matches();
  }

  private ContentCache cache() {
    ContentCache current = cache;
    if (current == null) {
      current = CACHES.computeIfAbsent(bounds(properties()), bounds -> {
        LOG.info("Table metadata cache: entries expire {} ms after access, {} bytes in total, files up to {} bytes",
            bounds.get(0), bounds.get(1), bounds.get(2));
        return new ContentCache(bounds.get(0), bounds.get(1), bounds.get(2));
      });
      cache = current;
    }
    return current;
  }

  /**
   * Expiry, total and per-file bounds of the cache. Entries expire an hour after their last access unless the catalog
   * says otherwise: Iceberg's 60 s default drops them between the queries of a session, so nothing is reused.
   */
  static List<Long> bounds(Map<String, String> properties) {
    return List.of(
        PropertyUtil.propertyAsLong(properties, CatalogProperties.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS,
            DEFAULT_EXPIRATION_MS),
        PropertyUtil.propertyAsLong(properties, CatalogProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES,
            CatalogProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES_DEFAULT),
        PropertyUtil.propertyAsLong(properties, CatalogProperties.IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH,
            CatalogProperties.IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH_DEFAULT));
  }
}
