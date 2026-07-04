/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.search.index.store;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hive.search.index.manifest.IndexManifest;

/** Remote index backup target backed by Hadoop {@link FileSystem}. */
public final class RemoteStateClient implements IndexStateClient {
  private final FileSystem fs;
  private final Path root;

  public RemoteStateClient(URI baseUri, Configuration conf, String indexName)
      throws IOException {
    this.fs = FileSystem.get(baseUri, conf);
    this.root = resolveRoot(baseUri, indexName);
    fs.mkdirs(root);
  }

  private static Path resolveRoot(URI baseUri, String indexName) {
    return new Path(new Path(baseUri), indexName);
  }

  @Override
  public Optional<IndexManifest> readManifest() throws IOException {
    Path manifestPath = new Path(root, IndexManifest.MANIFEST_FILE_NAME);
    if (!fs.exists(manifestPath)) {
      return Optional.empty();
    }
    try (InputStream in = fs.open(manifestPath)) {
      return Optional.of(IndexManifest.fromJson(in.readAllBytes()));
    }
  }

  @Override
  public InputStream read(String fileName) throws IOException {
    return fs.open(new Path(root, fileName));
  }

  @Override
  public void write(String fileName, InputStream stream) throws IOException {
    Path target = new Path(root, fileName);
    Path parent = target.getParent();
    if (parent != null && !fs.exists(parent)) {
      fs.mkdirs(parent);
    }
    try (OutputStream out = fs.create(target, true)) {
      stream.transferTo(out);
    }
  }

  @Override
  public void delete(String fileName) throws IOException {
    Path target = new Path(root, fileName);
    if (fs.exists(target)) {
      fs.delete(target, false);
    }
  }

  @Override
  public void clear() throws IOException {
    fs.delete(root, true);
    fs.mkdirs(root);
  }
}
