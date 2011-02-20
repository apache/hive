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
package org.apache.hadoop.hive.ql.metadata;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

/**
 * Test the partition class.
 */
public class TestPartition extends TestCase {

  private static final String PARTITION_COL = "partcol";
  private static final String PARTITION_VALUE = "value";
  private static final String TABLENAME = "tablename";

  /**
   * Test that the Partition spec is created properly.
   */
  public void testPartition() throws HiveException, URISyntaxException {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("partlocation");

    Partition tp = new Partition();
    tp.setTableName(TABLENAME);
    tp.setSd(sd);

    List<String> values = new ArrayList<String>();
    values.add(PARTITION_VALUE);
    tp.setValues(values);

    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema(PARTITION_COL, "string", ""));

    Table tbl = new Table("default", TABLENAME);
    tbl.setDataLocation(new URI("tmplocation"));
    tbl.setPartCols(partCols);

    Map<String, String> spec = new org.apache.hadoop.hive.ql.metadata.Partition(tbl, tp).getSpec();
    assertFalse(spec.isEmpty());
    assertEquals(spec.get(PARTITION_COL), PARTITION_VALUE);
  }

}
