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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Unit tests for the proxy {@link UserGroupInformation} cache used by {@link ServletSecurity} to bound the number of
 * proxy UGIs (and their associated FileSystem resources) that would otherwise leak through Hadoop's FileSystem cache.
 */
@Category(MetastoreUnitTest.class)
public class TestServletSecurity {

  private static Configuration confWithCache(long maxSize, long expirySeconds) {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setLongVar(conf, ConfVars.CATALOG_SERVLET_UGI_CACHE_SIZE, maxSize);
    MetastoreConf.setTimeVar(conf, ConfVars.CATALOG_SERVLET_UGI_CACHE_EXPIRY, expirySeconds, TimeUnit.SECONDS);
    return conf;
  }

  @Test
  public void testProxyUserIsCachedPerUser() throws Exception {
    ServletSecurity security = new ServletSecurity(ServletSecurity.AuthType.JWT, confWithCache(100, 3600));
    UserGroupInformation loginUser = UserGroupInformation.getCurrentUser();

    UserGroupInformation first = security.getProxyUser("alice", loginUser);
    UserGroupInformation second = security.getProxyUser("alice", loginUser);

    Assert.assertSame("Repeated requests for the same user must reuse the cached proxy UGI", first, second);
    Assert.assertEquals("alice", first.getShortUserName());
  }

  @Test
  public void testDistinctUsersGetDistinctProxies() throws Exception {
    ServletSecurity security = new ServletSecurity(ServletSecurity.AuthType.JWT, confWithCache(100, 3600));
    UserGroupInformation loginUser = UserGroupInformation.getCurrentUser();

    UserGroupInformation alice = security.getProxyUser("alice", loginUser);
    UserGroupInformation bob = security.getProxyUser("bob", loginUser);

    Assert.assertNotSame(alice, bob);
    Assert.assertEquals(2, security.proxyUserCacheSize());
  }

  @Test
  public void testEvictionClosesFileSystemForUgi() throws Exception {
    // A cache bounded to a single entry: inserting a second user evicts the first.
    ServletSecurity security = new ServletSecurity(ServletSecurity.AuthType.JWT, confWithCache(1, 3600));
    UserGroupInformation loginUser = UserGroupInformation.getCurrentUser();

    try (MockedStatic<FileSystem> fsMock = Mockito.mockStatic(FileSystem.class)) {
      UserGroupInformation alice = security.getProxyUser("alice", loginUser);
      security.getProxyUser("bob", loginUser);
      // Force any pending size-based eviction (and its synchronous cleanup) to run.
      security.cleanUpProxyUserCache();

      fsMock.verify(() -> FileSystem.closeAllForUGI(alice));
    }
  }

  @Test
  public void testExpiryDisabledWhenNonPositive() throws Exception {
    // expiry == 0 disables time-based eviction; the size bound still applies and entries remain until displaced.
    ServletSecurity security = new ServletSecurity(ServletSecurity.AuthType.JWT, confWithCache(100, 0));
    UserGroupInformation loginUser = UserGroupInformation.getCurrentUser();

    UserGroupInformation first = security.getProxyUser("carol", loginUser);
    UserGroupInformation second = security.getProxyUser("carol", loginUser);

    Assert.assertSame(first, second);
  }
}
