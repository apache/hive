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

package org.apache.hadoop.hive.common;

import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestStorageUtils {

  private final HiveConf conf = new HiveConf();

  @Before
  public void setUp() {
    conf.set(HiveConf.ConfVars.HIVE_BLOBSTORE_SUPPORTED_SCHEMES.varname, HiveConf.ConfVars.HIVE_BLOBSTORE_SUPPORTED_SCHEMES.getDefaultValue());
  }

  @Test
  public void testShouldInheritPerms() {
    conf.set(HiveConf.ConfVars.HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS.varname, "true");
    FileSystem fs = mock(FileSystem.class);
    for (String blobScheme : conf.getStringCollection(HiveConf.ConfVars.HIVE_BLOBSTORE_SUPPORTED_SCHEMES.varname)) {
      when(fs.getUri()).thenReturn(URI.create(blobScheme + ":///"));
      assertFalse(FileUtils.shouldInheritPerms(conf, fs));
    }

    when(fs.getUri()).thenReturn(URI.create("hdfs:///"));
    assertTrue(FileUtils.shouldInheritPerms(conf, fs));
  }
}
