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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

public interface IndexStateClient {

  Optional<IndexManifest> readManifest() throws IOException;

  InputStream read(String fileName) throws IOException;

  void write(String fileName, InputStream stream) throws IOException;

  void delete(String fileName) throws IOException;

  default boolean writeManifest(IndexManifest manifest) throws IOException {
    write(IndexManifest.MANIFEST_FILE_NAME, new ByteArrayInputStream(manifest.toJsonBytes()));
    return true;
  }

  default Optional<IndexManifest> readStagingManifest() throws IOException {
    return Optional.empty();
  }

  default void writeStagingManifest(IndexManifest manifest) throws IOException {

  }

  default void clearStagingManifest() throws IOException {

  }

  /** Snapshot of index files currently on disk, excluding staging metadata */
  default IndexManifest readLocalFileManifest() throws IOException {
    return readManifest().orElse(null);
  }

  default void validateRestoredIndex(IndexManifest expected) throws IOException {

  }

  void clear() throws IOException;
}
