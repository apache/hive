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

package org.apache.hive.jdbc;

import org.apache.hadoop.hive.llap.Row;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.hive.llap.LlapRowInputFormat;
import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.After;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.junit.Ignore;

/**
 * TestJdbcWithMiniLlap for llap Row format.
 */
@Ignore("HIVE-23549")
public class TestJdbcWithMiniLlapRow extends BaseJdbcWithMiniLlap {

  @BeforeClass
  public static void beforeTest() throws Exception {
    HiveConf conf = defaultConf();
    conf.setBoolVar(ConfVars.LLAP_OUTPUT_FORMAT_ARROW, false);
    BaseJdbcWithMiniLlap.beforeTest(conf);
  }

  @Override
  protected InputFormat<NullWritable, Row> getInputFormat() {
    return new LlapRowInputFormat();
  }

  @Override
  @Ignore
  public void testMultipleBatchesOfComplexTypes() {
    // ToDo: FixMe
  }

}

