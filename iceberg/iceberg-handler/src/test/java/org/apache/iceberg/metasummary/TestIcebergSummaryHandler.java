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

package org.apache.iceberg.metasummary;

import java.util.Arrays;
import java.util.Set;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metasummary.MetaSummarySchema;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergSummaryHandler {

  @Test
  public void testInitialize() throws Exception {
    try (IcebergSummaryHandler handler = new IcebergSummaryHandler()) {
      handler.setConf(MetastoreConf.newMetastoreConf());
      MetaSummarySchema schema = new MetaSummarySchema();
      handler.initialize("hive", true, schema);
      Set<String> fields = Sets.newHashSet(schema.getFields());
      Assertions.assertTrue(fields.containsAll(Arrays.asList("metadata", "stats",
          "CoW/MoR", "writeFormat", "distribution-mode")));
      schema = new MetaSummarySchema();
      handler.initialize("hive", false, schema);
      fields = Sets.newHashSet(schema.getFields());
      Assertions.assertTrue(fields.containsAll(Arrays.asList("puffin_enabled", "numSnapshots",
          "manifestsSize", "version", "write.distribution-mode", "write.format.default", "write.merge.mode")));
    }
  }
}
