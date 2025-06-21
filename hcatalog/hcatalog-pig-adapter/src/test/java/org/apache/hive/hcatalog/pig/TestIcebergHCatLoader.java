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
package org.apache.hive.hcatalog.pig;

import org.apache.hadoop.hive.ql.io.IOConstants;
import org.junit.After;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestIcebergHCatLoader extends AbstractHCatLoaderTest {
    static Logger LOG = LoggerFactory.getLogger(TestIcebergHCatLoader.class);
    @Override
    String getStorageFormat() {
        return IOConstants.ORC;
    }

    @Override
    public boolean isIcebergTable() {
        return true;
    }

    @Override
    @Ignore("Invalid for Iceberg table. Table size includes metadata file size")
    public void testGetInputBytesMultipleTables() throws Exception { }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        refreshDriver();
    }
}
