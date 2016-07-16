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
    @Test(timeout = 5000)
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
