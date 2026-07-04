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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hive.search.exception.IndexIOException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@Category(MetastoreUnitTest.class)
public class TestIndexStateConfig {

  @Test
  public void detectsRemoteAndMemoryFlags() {
    Configuration conf = new Configuration(false);
    conf.set(IndexStateConfig.REMOTE_URI, "file:///tmp/backup");
    conf.setBoolean(IndexStateConfig.MEMORY, true);
    IndexStateConfig config = new IndexStateConfig(conf, "hive_tables");
    assertTrue(config.hasRemote());
    assertTrue(config.isDistributed());
    assertTrue(config.useMemory());
  }

  @Test
  public void validateRemoteUriRejectsInvalidValue() {
    assertThrows(IndexIOException.class, () -> IndexStateConfig.validateRemoteUri("://bad"));
  }

  @Test
  public void localPathDefaultsToWorkdir() {
    IndexStateConfig config = new IndexStateConfig(new Configuration(false), "hive_tables");
    assertFalse(config.getLocalPath().toString().isEmpty());
  }
}
