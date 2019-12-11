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

package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.ql.io.AcidUtils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

/**
 * Transaction type derived from the original query test.
 */
@RunWith(value = Parameterized.class)
public class TestParseUtils {

  private String query;
  private TxnType txnType;
  private Configuration conf;

  public TestParseUtils(String query, TxnType txnType) {
    this.query = query;
    this.txnType = txnType;
    this.conf = new HiveConf();
  }

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
      new Object[][]{
          {"SELECT current_timestamp()", TxnType.READ_ONLY},
          {"SELECT count(*) FROM a", TxnType.READ_ONLY},
          {"SELECT count(*) FROM a JOIN b ON a.id = b.id", TxnType.READ_ONLY},

          {"WITH a AS (SELECT current_timestamp()) " +
             "  SELECT * FROM a", TxnType.READ_ONLY},

          {"INSERT INTO a VALUES (1, 2)", TxnType.DEFAULT},
          {"INSERT INTO a SELECT * FROM b", TxnType.DEFAULT},
          {"INSERT OVERWRITE TABLE a SELECT * FROM b", TxnType.DEFAULT},

          {"FROM b INSERT OVERWRITE TABLE a SELECT *", TxnType.DEFAULT},

          {"WITH a AS (SELECT current_timestamp()) " +
             "  INSERT INTO b SELECT * FROM a", TxnType.DEFAULT},

          {"UPDATE a SET col_b = 1", TxnType.DEFAULT},
          {"DELETE FROM a WHERE col_b = 1", TxnType.DEFAULT},

          {"CREATE TABLE a (col_b int)", TxnType.DEFAULT},
          {"CREATE TABLE a AS SELECT * FROM b", TxnType.DEFAULT},
          {"DROP TABLE a", TxnType.DEFAULT},

          {"LOAD DATA LOCAL INPATH './examples/files/kv.txt' " +
             "  OVERWRITE INTO TABLE a", TxnType.DEFAULT},

          {"REPL LOAD a from './examples/files/kv.txt'", TxnType.DEFAULT},
          {"REPL DUMP a", TxnType.DEFAULT},
          {"REPL STATUS a", TxnType.DEFAULT},

          {"MERGE INTO a trg using b src " +
             "  ON src.col_a = trg.col_a " +
             "WHEN MATCHED THEN " +
             "  UPDATE SET col_b = src.col_b " +
             "WHEN NOT MATCHED THEN " +
             "  INSERT VALUES (src.col_a, src.col_b)",
           TxnType.DEFAULT},
      });
  }

  @Test
  public void testTxnTypeWithEnabledReadOnlyFeature() throws ParseException {
    enableReadOnlyTxnFeature(true);
    Assert.assertEquals(AcidUtils.getTxnType(conf, ParseUtils.parse(query)), txnType);
  }

  @Test
  public void testTxnTypeWithDisabledReadOnlyFeature() throws ParseException {
    enableReadOnlyTxnFeature(false);
    Assert.assertEquals(AcidUtils.getTxnType(conf, ParseUtils.parse(query)), TxnType.DEFAULT);
  }

  private void enableReadOnlyTxnFeature(boolean featureFlag) {
    conf.setBoolean(HiveConf.ConfVars.HIVE_TXN_READONLY_ENABLED.varname, featureFlag);
  }
}
