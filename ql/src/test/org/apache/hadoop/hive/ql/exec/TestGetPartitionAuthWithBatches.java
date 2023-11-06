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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

import org.apache.hadoop.hive.metastore.api.GetPartitionsPsWithAuthRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestGetPartitionAuthWithBatches {

    private final String catName = "hive";
    private final String dbName = "default";
    private final String tableName = "test_partition_batch_with_auth";
    private static HiveConf hiveConf;
    private static HiveMetaStoreClient msc;
    private static Hive hive;
    private Table table;
    private final static int NUM_PARTITIONS = 30;
    private final static int DECAYING_FACTOR = 2;
    private final static int MAX_RETRIES = 0;
    private final static boolean IS_AUTH_REQUIRED = true;
    private final static String USER_NAME = "username";
    private final static List<String> GROUP_NAMES = Arrays.asList("Grp1", "Grp2");
    private final static Map<String, String> PARTIAL_PARTITION_SPEC = null;

    @BeforeClass
    public static void setupClass() throws HiveException {
        hiveConf = new HiveConf(TestGetPartitionAuthWithBatches.class);
        hiveConf.set("hive.security.authorization.enabled", "true");
        hiveConf.set("hive.security.authorization.manager","org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider");
        hive = Hive.get();
        SessionState.start(hiveConf);
        try {
            msc = new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            throw new HiveException(e);
        }
    }

    @Before
    public void before() throws Exception {
        PartitionUtil.createPartitionedTable(msc, catName, dbName, tableName);
        table = msc.getTable(catName, dbName, tableName);
        PartitionUtil.addPartitions(msc, dbName, tableName, table.getSd().getLocation(), hiveConf, NUM_PARTITIONS);
    }

    @After
    public void after() throws Exception {
        PartitionUtil.cleanUpTableQuietly(msc, catName, dbName, tableName);
    }

    @Test
    public void testNumberOfPartitionsRetrieved() throws HiveException {
        List<String> numParts = hive.getPartitionNames(dbName, tableName, (short)-1);
        Assert.assertEquals(numParts.size(), NUM_PARTITIONS);
        List<Partition> partitions = hive.getPartitionsAuthByNames(new org.apache.hadoop.hive.ql.metadata.Table(table),
                numParts.subList(0,5), USER_NAME, GROUP_NAMES);
        Assert.assertEquals(partitions.size(), 5);
    }

    /**
     * Tests the number of partitions recieved from the HMS
     *
     * @throws Exception
     */
    @Test
    public void testGetPartitionsAPI() throws Exception {
        List<org.apache.hadoop.hive.ql.metadata.Partition> part = hive.getPartitions(hive.getTable(dbName, tableName));
        Assert.assertEquals(part.size(), NUM_PARTITIONS);
    }

    @Test
    public void testGetPartitionsAPI2() throws Exception {
        List<org.apache.hadoop.hive.ql.metadata.Partition> part = hive.getPartitions(hive.getTable(dbName, tableName),
                new HashMap() , (short) -1);
        Assert.assertEquals(part.size(), NUM_PARTITIONS);
    }

    @Test
    public void testGetPartitionsAPI2limit() throws Exception {
        List<org.apache.hadoop.hive.ql.metadata.Partition> part = hive.getPartitions(hive.getTable(dbName, tableName),
                new HashMap() , (short) 1);
        Assert.assertEquals(part.size(), 1);

        List<org.apache.hadoop.hive.ql.metadata.Partition> part1 = hive.getPartitions(hive.getTable(dbName, tableName),
                new HashMap() , (short) 10);
        Assert.assertEquals(part1.size(), 10);
    }

    /**
     * Tests the number of times Hive.getPartitions calls are executed with total number of
     * partitions to be added are equally divisible by batch size
     *
     * @throws Exception
     */
    @Test
    public void testNumberOfGetPartitionCalls() throws Exception {
        HiveMetaStoreClient spyMSC = spy(msc);
        hive.setMSC(spyMSC);
        // test with a batch size of 10 and decaying factor of 2
        int batchSize = 10;
        hive.getAllPartitionsInBatches(hive.getTable(dbName, tableName), batchSize, DECAYING_FACTOR, MAX_RETRIES,
                PARTIAL_PARTITION_SPEC, IS_AUTH_REQUIRED, USER_NAME, GROUP_NAMES);
        ArgumentCaptor<GetPartitionsPsWithAuthRequest> req = ArgumentCaptor.forClass(GetPartitionsPsWithAuthRequest.class);
        // there should be 3 calls to get partitions
        verify(spyMSC, times(3)).listPartitionsWithAuthInfoRequest(req.capture());
        req.getAllValues().forEach(part-> Assert.assertEquals(part.getPartNames().size(),10));
    }

    @Test
    public void testNumberOfGetPartitionCalls2() throws Exception {
        HiveMetaStoreClient spyMSC = spy(msc);
        hive.setMSC(spyMSC);
        // test with a batch size of 10 and decaying factor of 2
        int batchSize = 10;
        hive.getAllPartitionsInBatches(hive.getTable(dbName, tableName), batchSize, DECAYING_FACTOR, MAX_RETRIES,
                new HashMap(), IS_AUTH_REQUIRED, USER_NAME, GROUP_NAMES);
        ArgumentCaptor<GetPartitionsPsWithAuthRequest> req = ArgumentCaptor.forClass(GetPartitionsPsWithAuthRequest.class);
        // there should be 3 calls to get partitions
        verify(spyMSC, times(3)).listPartitionsWithAuthInfoRequest(req.capture());
        req.getAllValues().forEach(part-> Assert.assertEquals(part.getPartNames().size(), 10));
    }

    /**
     * Tests the number of times Hive.getAllPartitionsOf calls are executed with total number of
     * partitions to be added are not exactly divisible by batch size
     *
     * @throws Exception
     */
    @Test
    public void testUnevenNumberOfGetPartitionCalls() throws Exception {
        HiveMetaStoreClient spyMSC = spy(msc);
        hive.setMSC(spyMSC);
        // there should be 2 calls to get partitions with batch sizes of 19, 11
        int batchSize = 19;
        hive.getAllPartitionsInBatches(hive.getTable(dbName, tableName),batchSize, DECAYING_FACTOR, MAX_RETRIES,
                PARTIAL_PARTITION_SPEC, IS_AUTH_REQUIRED, USER_NAME, GROUP_NAMES);
        ArgumentCaptor<GetPartitionsPsWithAuthRequest> req = ArgumentCaptor.forClass(GetPartitionsPsWithAuthRequest.class);
        // there should be 2 calls to get partitions
        verify(spyMSC, times(2)).listPartitionsWithAuthInfoRequest(req.capture());
        // confirm the batch sizes were 19, 11 in the two calls to get partitions
        List<GetPartitionsPsWithAuthRequest> apds = req.getAllValues();
        Assert.assertEquals(19, apds.get(0).getPartNames().size());
        Assert.assertEquals(11, apds.get(1).getPartNames().size());
    }

    /**
     * Tests the number of times Hive.getAllPartitionsOf calls are executed with total number of
     * partitions to is less than batch size
     *
     * @throws Exception
     */
    @Test
    public void testSmallNumberOfPartitions() throws Exception {
        HiveMetaStoreClient spyMSC = spy(msc);
        hive.setMSC(spyMSC);
        int batchSize = 100;
        hive.getAllPartitionsInBatches(hive.getTable(dbName, tableName),batchSize, DECAYING_FACTOR, MAX_RETRIES,
                PARTIAL_PARTITION_SPEC, IS_AUTH_REQUIRED, USER_NAME, GROUP_NAMES);
        ArgumentCaptor<GetPartitionsPsWithAuthRequest> req = ArgumentCaptor.forClass(GetPartitionsPsWithAuthRequest.class);
        // there should be 1 call to get partitions
        verify(spyMSC, times(1)).listPartitionsWithAuthInfoRequest(req.capture());
        Assert.assertEquals(NUM_PARTITIONS, req.getValue().getPartNames().size());
    }

    /**
     * Tests the retries exhausted case when getAllPartitionsOf method call always keep throwing
     * HiveException. The batch sizes should exponentially decreased based on the decaying factor and
     * ultimately give up when it reaches 0
     *
     * @throws Exception
     */
    @Test
    public void testRetriesExhaustedBatchSize() throws Exception {
        HiveMetaStoreClient spyMSC = spy(msc);
        hive.setMSC(spyMSC);
        doThrow(MetaException.class).when(spyMSC).listPartitionsWithAuthInfoRequest(any());
        try {
            hive.getAllPartitionsInBatches(hive.getTable(dbName, tableName), NUM_PARTITIONS, DECAYING_FACTOR, MAX_RETRIES,
                    PARTIAL_PARTITION_SPEC, IS_AUTH_REQUIRED, USER_NAME, GROUP_NAMES);
        } catch (HiveException ignored) {}
        ArgumentCaptor<GetPartitionsPsWithAuthRequest> req = ArgumentCaptor.forClass(GetPartitionsPsWithAuthRequest.class);
        // there should be 5 call to get partitions with batch sizes as 30, 15, 7, 3, 1
        verify(spyMSC, times(5)).listPartitionsWithAuthInfoRequest(req.capture());
        List<GetPartitionsPsWithAuthRequest> apds = req.getAllValues();
        Assert.assertEquals(5, apds.size());

        Assert.assertEquals(30, apds.get(0).getPartNamesSize());
        Assert.assertEquals(15, apds.get(1).getPartNamesSize());
        Assert.assertEquals(7, apds.get(2).getPartNamesSize());
        Assert.assertEquals(3, apds.get(3).getPartNamesSize());
        Assert.assertEquals(1, apds.get(4).getPartNamesSize());
    }

    /**
     * Tests the maximum retry attempts provided by configuration
     * @throws Exception
     */
    @Test
    public void testMaxRetriesReached() throws Exception {
        HiveMetaStoreClient spyMSC = spy(msc);
        hive.setMSC(spyMSC);
        doThrow(MetaException.class).when(spyMSC).listPartitionsWithAuthInfoRequest(any());
        int maxRetries = 2;
        try {
            hive.getAllPartitionsInBatches(hive.getTable(dbName, tableName), NUM_PARTITIONS, DECAYING_FACTOR, maxRetries,
                    PARTIAL_PARTITION_SPEC, IS_AUTH_REQUIRED, USER_NAME, GROUP_NAMES);
        } catch (HiveException ignored) {}
        ArgumentCaptor<GetPartitionsPsWithAuthRequest> req = ArgumentCaptor.forClass(GetPartitionsPsWithAuthRequest.class);
        // there should be 2 call to get partitions with batch sizes as 30, 15
        verify(spyMSC, times(2)).listPartitionsWithAuthInfoRequest(req.capture());
        List<GetPartitionsPsWithAuthRequest> apds = req.getAllValues();
        Assert.assertEquals(2, apds.size());

        Assert.assertEquals(30, apds.get(0).getPartNamesSize());
        Assert.assertEquals(15, apds.get(1).getPartNamesSize());
    }

    /**
     * Tests the number of calls to getPartitions and the respective batch sizes when first call to
     * getPartitions throws HiveException. The batch size should be reduced by the decayingFactor
     * and the second call should fetch all the results
     *
     * @throws Exception
     */
    @Test
    public void testBatchingWhenException() throws Exception {
        HiveMetaStoreClient spyMSC = spy(msc);
        hive.setMSC(spyMSC);
        // This will throw exception only the first time.
        doThrow(new MetaException()).doCallRealMethod()
                .when(spyMSC).listPartitionsWithAuthInfoRequest(any());

        int maxRetries = 5;
        hive.getAllPartitionsInBatches(hive.getTable(dbName, tableName), NUM_PARTITIONS, DECAYING_FACTOR, maxRetries,
                PARTIAL_PARTITION_SPEC, IS_AUTH_REQUIRED, USER_NAME, GROUP_NAMES);
        ArgumentCaptor<GetPartitionsPsWithAuthRequest> req = ArgumentCaptor.forClass(GetPartitionsPsWithAuthRequest.class);
        // The first call with batch size of 30 will fail, the rest two call will be of size 15 each. Total 3 calls
        verify(spyMSC, times(3)).listPartitionsWithAuthInfoRequest(req.capture());
        List<GetPartitionsPsWithAuthRequest> apds = req.getAllValues();
        Assert.assertEquals(3, apds.size());

        Assert.assertEquals(30, apds.get(0).getPartNamesSize());
        Assert.assertEquals(15, apds.get(1).getPartNamesSize());
        Assert.assertEquals(15, apds.get(2).getPartNamesSize());

        Set<String> partNames = new HashSet<>(apds.get(1).getPartNames());
        partNames.addAll(apds.get(2).getPartNames());
        assert(partNames.size() == NUM_PARTITIONS);

        List<String> partitionNames = hive.getPartitionNames(table.getDbName(),table.getTableName(), (short) -1);
        assert(partitionNames.size() == NUM_PARTITIONS);
        partitionNames.forEach(partNames::remove);
        assert(partitionNames.size() == NUM_PARTITIONS);
        // In case any duplicate/incomplete list is given by hive.getAllPartitionsInBatches, the below assertion will fail
        assert(partNames.size() == 0);
    }
}
