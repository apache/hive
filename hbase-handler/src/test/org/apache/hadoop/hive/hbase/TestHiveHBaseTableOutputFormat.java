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

package org.apache.hadoop.hive.hbase;

import static org.junit.Assert.assertTrue;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.junit.Test;
import org.mockito.Mockito;

/**
 *  This is a simple test to make sure HiveHBaseTableOutputFormat implements HiveOutputFormat for HBase tables.
 */
public class TestHiveHBaseTableOutputFormat {

  @Test
  public void testInstanceOfHiveHBaseTableOutputFormat() {
    HiveHBaseTableOutputFormat hBaseOutputFormat = Mockito.mock(HiveHBaseTableOutputFormat.class);
    assertTrue(hBaseOutputFormat instanceof TableOutputFormat);
    assertTrue(hBaseOutputFormat instanceof HiveOutputFormat);
  }
}
