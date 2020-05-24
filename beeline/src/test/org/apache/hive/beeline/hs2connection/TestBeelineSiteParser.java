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
package org.apache.hive.beeline.hs2connection;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.hive.common.util.HiveTestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestBeelineSiteParser {
    @Test
    public void testConfigLocationPathInEtc() throws Exception {
        BeelineSiteParser testHS2ConfigManager =
                new BeelineSiteParser();
        Field locations = testHS2ConfigManager.getClass().getDeclaredField("locations");
        locations.setAccessible(true);
        Collection<String> locs = (List<String>)locations.get(testHS2ConfigManager);
        Assert.assertTrue(locs.contains(
                BeelineSiteParser.ETC_HIVE_CONF_LOCATION +
                        File.separator +
                        BeelineSiteParser.DEFAULT_BEELINE_SITE_FILE_NAME));

    }

    @Test
    public void testConnectionURLWithVarSubsitition() throws Exception {
        List<String> locations = new ArrayList<String>();
        String beelineSite = HiveTestUtils.getFileFromClasspath(BeelineSiteParser.DEFAULT_BEELINE_SITE_FILE_NAME);
        locations.add(beelineSite);
        BeelineSiteParser beelineSiteParser = new BeelineSiteParser(locations);
        Properties properties = beelineSiteParser.getConnectionProperties();
        Assert.assertEquals("jdbc:hive2://zkhost1:2181,zkhost2:2181,zkhost3:2181/;" +
                        "serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2?tez.queue.name=myqueue",
                properties.get("test"));;
    }


}
