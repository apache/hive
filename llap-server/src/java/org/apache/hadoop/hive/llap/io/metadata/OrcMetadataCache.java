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

package org.apache.hadoop.hive.llap.io.metadata;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.ql.io.orc.StripeInformation;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * ORC-specific metadata cache.
 */
public class OrcMetadataCache {
  private static final int DEFAULT_CACHE_ACCESS_CONCURRENCY = 10;
  private static final int DEFAULT_MAX_CACHE_ENTRIES = 100;
  private static Cache<String, OrcMetadata> METADATA;

  static {
    METADATA = CacheBuilder.newBuilder()
        .concurrencyLevel(DEFAULT_CACHE_ACCESS_CONCURRENCY)
        .maximumSize(DEFAULT_MAX_CACHE_ENTRIES)
        .build();
  }

  private Path path;
  private OrcMetadataLoader loader;

  public OrcMetadataCache(FileSystem fs, Path path, Configuration conf) {
    this.path = path;
    this.loader = new OrcMetadataLoader(fs, path, conf);
  }

  public CompressionKind getCompression(String pathString) throws IOException {
    try {
      return METADATA.get(pathString, loader).getCompressionKind();
    } catch (ExecutionException e) {
      throw new IOException("Unable to load orc metadata for " + path.toString(), e);
    }
  }

  public int getCompressionBufferSize(String pathString) throws IOException {
    try {
      return METADATA.get(pathString, loader).getCompressionBufferSize();
    } catch (ExecutionException e) {
      throw new IOException("Unable to load orc metadata for " + path.toString(), e);
    }
  }

  public List<OrcProto.Type> getTypes(String pathString) throws IOException {
    try {
      return METADATA.get(pathString, loader).getTypes();
    } catch (ExecutionException e) {
      throw new IOException("Unable to load orc metadata for " + path.toString(), e);
    }
  }

  public List<StripeInformation> getStripes(String pathString) throws IOException {
    try {
      return METADATA.get(pathString, loader).getStripes();
    } catch (ExecutionException e) {
      throw new IOException("Unable to load orc metadata for " + path.toString(), e);
    }
  }

  //  public boolean[] getIncludedRowGroups(String pathString, SearchArgument sarg, int stripeIdx) throws IOException {
  //    try {
  //      return METADATA.get(pathString, loader).getStripeToRowIndexEntries();
  //    } catch (ExecutionException e) {
  //      throw new IOException("Unable to load orc metadata for " + path.toString(), e);
  //    }
  //  }
}
