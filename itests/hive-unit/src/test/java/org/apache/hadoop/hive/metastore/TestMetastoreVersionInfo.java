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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.utils.MetastoreVersionInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestMetastoreVersionInfo {
    private static final Logger LOG = LoggerFactory.getLogger(TestMetastoreVersionInfo.class);
   private String hiveShortVersion;
   private String hiveVersion;

    @Before
    public void setUp() throws Exception {
        hiveShortVersion = MetastoreVersionInfo.getShortVersion();
        hiveVersion = MetastoreVersionInfo.getVersion();
    }

    @Test
    public void testValidateHiveShortVersionWithHiveVersion() {
        Assert.assertEquals(hiveVersion.replace("-SNAPSHOT", ""), hiveShortVersion);
    }

    @Test
    public void testIfHiveVersionHasShortVersionAsPrefix() {
        Assert.assertTrue(hiveVersion.startsWith(hiveShortVersion));
    }
}
