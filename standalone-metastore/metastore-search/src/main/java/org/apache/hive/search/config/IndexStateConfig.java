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

package org.apache.hive.search.config;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.search.exception.IndexIOException;

public record IndexStateConfig(Configuration configuration, String indexName) {
  public static final String LOCAL_PATH = "metastore.index.local.path";
  public static final String REMOTE_URI = "metastore.index.backup.remote.uri";
  public static final String MEMORY = "metastore.index.use.memory";

  public static final Path DEFAULT_WORKDIR = Paths.get(System.getProperty("user.dir"), "indexes");

  public Path getLocalPath() {
    String path = configuration.get(LOCAL_PATH, DEFAULT_WORKDIR.toString());
    return Path.of(path);
  }

  public String getRemoteUri() {
    return configuration.get(REMOTE_URI, "");
  }

  public boolean hasRemote() {
    String uri = getRemoteUri();
    return StringUtils.isNotEmpty(uri);
  }

  public boolean useMemory() {
    return configuration.getBoolean(MEMORY, false);
  }

  public boolean isDistributed() {
    return hasRemote();
  }

  public static void validateRemoteUri(String uriText) throws IndexIOException {
    try {
      URI.create(uriText);
    } catch (IllegalArgumentException e) {
      throw new IndexIOException("invalid store.remote.uri: " + uriText, e);
    }
  }
}
