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

package org.apache.hadoop.hive.ql.exec.tez;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastTableContainer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.ConvertJoinMapJoin;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableImplementationType;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKind;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;
import org.openjdk.jol.info.GraphLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestVectorMapJoinFastHashTable {

  // TODO HIVE-25145
  long keyCount = 15_000_000;

  private static final Logger LOG = LoggerFactory.getLogger(TestVectorMapJoinFastHashTable.class.getName());

  @Test
  public void checkFast2estimations() throws Exception {
    runEstimationCheck(HashTableKeyType.LONG);
  }

  @Test
  public void checkFast3estimations() throws Exception {
    runEstimationCheck(HashTableKeyType.MULTI_KEY);
  }

  private void runEstimationCheck(HashTableKeyType l) throws SerDeException, IOException, HiveException {
    MapJoinDesc desc = new MapJoinDesc();
    VectorMapJoinDesc vectorDesc = new VectorMapJoinDesc();
    vectorDesc.setHashTableKeyType(l);
    vectorDesc.setIsFastHashTableEnabled(true);
    vectorDesc.setHashTableImplementationType(HashTableImplementationType.FAST);
    vectorDesc.setHashTableKind(HashTableKind.HASH_MAP);
    desc.setVectorDesc(vectorDesc);
    TableDesc keyTblDesc = new TableDesc();
    keyTblDesc.setProperties(new Properties());
    desc.setKeyTblDesc(keyTblDesc);
    Configuration hconf = new HiveConf();
    VectorMapJoinFastTableContainer container = new VectorMapJoinFastTableContainer(desc, hconf, keyCount, 1);

    container.setSerde(null, null);

    long dataSize = 0;

    BinarySortableSerializeWrite bsw = new BinarySortableSerializeWrite(1);

    Output outp = new Output();
    BytesWritable key;
    BytesWritable value;
    for (int i = 0; i < keyCount; i++) {
      bsw.set(outp);
      bsw.writeLong(i);
      key = new BytesWritable(outp.getData(), outp.getLength());
      bsw.set(outp);
      bsw.writeLong(i * 2L);
      value = new BytesWritable(outp.getData(), outp.getLength());

      container.putRow(key, value);
      dataSize += 8;
      dataSize += 8;
    }

    Statistics stat = new Statistics(keyCount, dataSize, 0, 0);

    long realObjectSize = GraphLayout.parseInstance(container).totalSize();
    long executionEstimate = container.getEstimatedMemorySize();

    ConvertJoinMapJoin cjm = new ConvertJoinMapJoin();
    cjm.hashTableLoadFactor = .75f;

    long compilerEstimate = switch (l) {
      case MULTI_KEY -> cjm.computeOnlineDataSizeFastCompositeKeyed(stat);
      case LONG -> cjm.computeOnlineDataSizeFastLongKeyed(stat);
      default -> throw new AssertionError("Unexpected HashTableKeyType: " + l);
    };
    LOG.info("stats: {}", stat);
    LOG.info("realObjectSize: {}", realObjectSize);
    LOG.info("executionEstimate : {}", executionEstimate);
    LOG.info("compilerEstimate: {}", compilerEstimate);

    checkRelativeError(realObjectSize, executionEstimate, .06);
    checkRelativeError(realObjectSize, compilerEstimate, .11);
    checkRelativeError(compilerEstimate, executionEstimate, .05);
  }

  private void checkRelativeError(Long v1, Long v2, double err) {
    if (v1 == null || v2 == null) {
      return;
    }
    double d = (double) v1 / v2;
    assertEquals("error is outside of tolerance margin", 1.0, d, err);
  }
}
