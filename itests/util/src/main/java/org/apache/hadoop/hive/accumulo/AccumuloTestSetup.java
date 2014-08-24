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
package org.apache.hadoop.hive.accumulo;

import java.io.File;
import java.sql.Date;
import java.sql.Timestamp;

import junit.extensions.TestSetup;
import junit.framework.Test;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * Start and stop an AccumuloMiniCluster for testing purposes
 */
public class AccumuloTestSetup extends TestSetup {
  public static final String PASSWORD = "password";
  public static final String TABLE_NAME = "accumuloHiveTable";

  protected MiniAccumuloCluster miniCluster;

  public AccumuloTestSetup(Test test) {
    super(test);
  }

  protected void setupWithHiveConf(HiveConf conf) throws Exception {
    if (null == miniCluster) {
      String testTmpDir = System.getProperty("test.tmp.dir");
      File tmpDir = new File(testTmpDir, "accumulo");

      MiniAccumuloConfig cfg = new MiniAccumuloConfig(tmpDir, PASSWORD);
      cfg.setNumTservers(1);

      miniCluster = new MiniAccumuloCluster(cfg);

      miniCluster.start();

      createAccumuloTable(miniCluster.getConnector("root", PASSWORD));
    }

    // Setup connection information
    conf.set(AccumuloConnectionParameters.USER_NAME, "root");
    conf.set(AccumuloConnectionParameters.USER_PASS, PASSWORD);
    conf.set(AccumuloConnectionParameters.ZOOKEEPERS, miniCluster.getZooKeepers());
    conf.set(AccumuloConnectionParameters.INSTANCE_NAME, miniCluster.getInstanceName());
  }

  protected void createAccumuloTable(Connector conn) throws TableExistsException,
      TableNotFoundException, AccumuloException, AccumuloSecurityException {
    TableOperations tops = conn.tableOperations();
    if (tops.exists(TABLE_NAME)) {
      tops.delete(TABLE_NAME);
    }

    tops.create(TABLE_NAME);

    boolean[] booleans = new boolean[] {true, false, true};
    byte [] bytes = new byte [] { Byte.MIN_VALUE, -1, Byte.MAX_VALUE };
    short [] shorts = new short [] { Short.MIN_VALUE, -1, Short.MAX_VALUE };
    int [] ints = new int [] { Integer.MIN_VALUE, -1, Integer.MAX_VALUE };
    long [] longs = new long [] { Long.MIN_VALUE, -1, Long.MAX_VALUE };
    String [] strings = new String [] { "Hadoop, Accumulo", "Hive", "Test Strings" };
    float [] floats = new float [] { Float.MIN_VALUE, -1.0F, Float.MAX_VALUE };
    double [] doubles = new double [] { Double.MIN_VALUE, -1.0, Double.MAX_VALUE };
    HiveDecimal[] decimals = new HiveDecimal[] {HiveDecimal.create("3.14159"), HiveDecimal.create("2.71828"), HiveDecimal.create("0.57721")};
    Date[] dates = new Date[] {Date.valueOf("2014-01-01"), Date.valueOf("2014-03-01"), Date.valueOf("2014-05-01")};
    Timestamp[] timestamps = new Timestamp[] {new Timestamp(50), new Timestamp(100), new Timestamp(150)};

    BatchWriter bw = conn.createBatchWriter(TABLE_NAME, new BatchWriterConfig());
    final String cf = "cf";
    try {
      for (int i = 0; i < 3; i++) {
        Mutation m = new Mutation("key-" + i);
        m.put(cf, "cq-boolean", Boolean.toString(booleans[i]));
        m.put(cf.getBytes(), "cq-byte".getBytes(), new byte[] {bytes[i]});
        m.put(cf, "cq-short", Short.toString(shorts[i]));
        m.put(cf, "cq-int", Integer.toString(ints[i]));
        m.put(cf, "cq-long", Long.toString(longs[i]));
        m.put(cf, "cq-string", strings[i]);
        m.put(cf, "cq-float", Float.toString(floats[i]));
        m.put(cf, "cq-double", Double.toString(doubles[i]));
        m.put(cf, "cq-decimal", decimals[i].toString());
        m.put(cf, "cq-date", dates[i].toString());
        m.put(cf, "cq-timestamp", timestamps[i].toString());

        bw.addMutation(m);
      }
    } finally {
      bw.close();
    }
  }

  @Override
  protected void tearDown() throws Exception {
    if (null != miniCluster) {
      miniCluster.stop();
      miniCluster = null;
    }
  }
}
