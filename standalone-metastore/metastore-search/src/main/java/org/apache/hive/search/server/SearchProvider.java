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

package org.apache.hive.search.server;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.search.exception.IndexNotReadyException;
import org.apache.hive.search.exception.InitializeException;
import org.apache.hive.search.exception.SearchException;
import org.apache.hive.search.search.LuceneSearchBackend;
import org.apache.hive.search.search.SearchBackend;
import org.apache.hive.search.search.SearchQuery;
import org.apache.hive.search.search.TableSearchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Single server-side provider that owns the lifecycle of the {@link SearchBackend}. */
public final class SearchProvider implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(SearchProvider.class);
  private static final AtomicReference<SearchProvider> INSTANCE = new AtomicReference<>();

  private final Configuration configuration;
  private final SearchBackend backend;

  private SearchProvider(Configuration configuration, SearchBackend backend) {
    this.configuration = configuration;
    this.backend = backend;
  }

  /** Creates and initializes a provider with the supplied backend implementation. */
  private static SearchProvider create(Configuration configuration, SearchBackend backend)
      throws InitializeException, IOException {
    Objects.requireNonNull(configuration, "configuration");
    Objects.requireNonNull(backend, "backend");
    backend.initialize(configuration);
    LOG.info("Installed search backend {}", backend.getClass().getSimpleName());
    return new SearchProvider(configuration, backend);
  }

  /** Installs the process-wide provider, replacing any previous instance. */
  public static SearchProvider install(Configuration configuration)
      throws InitializeException, IOException {
    return install(configuration, new LuceneSearchBackend());
  }

  /** Installs the process-wide provider using the supplied backend implementation. */
  public static SearchProvider install(Configuration configuration, SearchBackend backend)
      throws InitializeException, IOException {
    SearchProvider provider;
    if ((provider = INSTANCE.get()) != null) {
      return provider;
    }
    synchronized (SearchProvider.class) {
      if ((provider = INSTANCE.get()) == null) {
        provider = create(configuration, backend);
        INSTANCE.getAndSet(provider);
      }
    }
    return provider;
  }

  /** Returns the installed provider. */
  public static SearchProvider get() {
    SearchProvider provider = INSTANCE.get();
    if (provider == null) {
      throw new IllegalStateException("SearchServerProvider is not installed");
    }
    return provider;
  }

  /** Clears the installed provider; intended for tests. */
  public static void reset() throws Exception {
    SearchProvider previous = INSTANCE.getAndSet(null);
    if (previous != null) {
      previous.close();
    }
  }

  public Configuration configuration() {
    return configuration;
  }

  public SearchBackend backend() {
    return backend;
  }

  public boolean isReady() throws IndexNotReadyException {
    return backend.isReady();
  }

  public TableSearchResult search(SearchQuery query)
      throws SearchException, InitializeException, IOException {
    return backend.search(query);
  }

  @Override
  public void close() throws Exception {
    backend.close();
  }
}
