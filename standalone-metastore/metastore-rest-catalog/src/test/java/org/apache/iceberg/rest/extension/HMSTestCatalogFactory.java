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

package org.apache.iceberg.rest.extension;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.ServletServerBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.rest.HMSCatalogFactory;

import javax.servlet.http.HttpServlet;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Factory for creating a test catalog that caches the last created catalog.
 * <p>This class is used in tests to verify the behavior of the caching catalog.</p>
 */
public class HMSTestCatalogFactory extends HMSCatalogFactory {
  static final AtomicReference<Reference<HMSTestCachingCatalog>> catRef = new AtomicReference<>(null);
  /**
   * Factory constructor.
   * <p>Called by the static method {@link HMSTestCatalogFactory#createServlet(Configuration)} that is
   * declared in configuration and found through introspection.</p>
   *
   * @param conf the configuration
   */
  protected HMSTestCatalogFactory(Configuration conf) {
    super(conf);
  }

  @Override
  protected Catalog cacheCatalog(HiveCatalog hiveCatalog) {
    long expiry = MetastoreConf.getLongVar(configuration, MetastoreConf.ConfVars.ICEBERG_CATALOG_CACHE_EXPIRY);
    HMSTestCachingCatalog cc = new HMSTestCachingCatalog(hiveCatalog, expiry);
    catRef.set(new SoftReference<>(cc));
    return cc;
  }

  public static HMSTestCachingCatalog getLastCatalog() {
    Reference<HMSTestCachingCatalog> ref = catRef.get();
    return ref.get();
  }

  public static void clearLastCatalog() {
    catRef.set(null);
  }

  /**
   * Creates the servlet instance.
   * @return the servlet
   */
  public static ServletServerBuilder.Descriptor createServlet(Configuration configuration) {
    HMSTestCatalogFactory hms = new HMSTestCatalogFactory(configuration);
    HttpServlet servlet = hms.createServlet();
    if (servlet != null) {
      return new ServletServerBuilder.Descriptor(hms.getPort(), hms.getPath(), servlet);
    }
    return null;
  }
}
