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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCustomPartitionVertex {
    @Test(timeout = 20000)
    public void testGetBytePayload() throws IOException {
        int numBuckets = 10;
        VertexManagerPluginContext context = mock(VertexManagerPluginContext.class);
        CustomVertexConfiguration vertexConf =
                new CustomVertexConfiguration(numBuckets, TezWork.VertexType.INITIALIZED_EDGES);
        DataOutputBuffer dob = new DataOutputBuffer();
        vertexConf.write(dob);
        UserPayload payload = UserPayload.create(ByteBuffer.wrap(dob.getData()));
        when(context.getUserPayload()).thenReturn(payload);

        CustomPartitionVertex vm = new CustomPartitionVertex(context);
        vm.initialize();

        // prepare empty routing table
        Multimap<Integer, Integer> routingTable = HashMultimap.<Integer, Integer> create();
        payload = vm.getBytePayload(routingTable);
        // get conf from user payload
        CustomEdgeConfiguration edgeConf = new CustomEdgeConfiguration();
        DataInputByteBuffer dibb = new DataInputByteBuffer();
        dibb.reset(payload.getPayload());
        edgeConf.readFields(dibb);
        assertEquals(numBuckets, edgeConf.getNumBuckets());
    }
}
