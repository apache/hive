/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.tez;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.EntityDescriptorProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.IOSpecProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryIdentifierProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SignableVertexSpec;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.UserPayloadProto;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.junit.Test;

public class TestConverters {

  @Test(timeout = 10000)
  public void testTaskSpecToFragmentSpec() {
    ByteBuffer procBb = ByteBuffer.allocate(4);
    procBb.putInt(0, 200);
    UserPayload processorPayload = UserPayload.create(procBb);
    ProcessorDescriptor processorDescriptor =
        ProcessorDescriptor.create("fakeProcessorName").setUserPayload(processorPayload);

    ByteBuffer input1Bb = ByteBuffer.allocate(4);
    input1Bb.putInt(0, 300);
    UserPayload input1Payload = UserPayload.create(input1Bb);
    InputDescriptor id1 = InputDescriptor.create("input1ClassName").setUserPayload(input1Payload);
    InputSpec inputSpec1 = new InputSpec("sourceVertexName1", id1, 33);
    InputSpec inputSpec2 = new InputSpec("sourceVertexName2", id1, 44);
    List<InputSpec> inputSpecList = Lists.newArrayList(inputSpec1, inputSpec2);

    ByteBuffer output1Bb = ByteBuffer.allocate(4);
    output1Bb.putInt(0, 400);
    UserPayload output1Payload = UserPayload.create(output1Bb);
    OutputDescriptor od1 =
        OutputDescriptor.create("output1ClassName").setUserPayload(output1Payload);
    OutputSpec outputSpec1 = new OutputSpec("destVertexName1", od1, 55);
    OutputSpec outputSpec2 = new OutputSpec("destVertexName2", od1, 66);
    List<OutputSpec> outputSpecList = Lists.newArrayList(outputSpec1, outputSpec2);

    ApplicationId appId = ApplicationId.newInstance(1000, 100);
    TezDAGID tezDagId = TezDAGID.getInstance(appId, 300);
    TezVertexID tezVertexId = TezVertexID.getInstance(tezDagId, 400);
    TezTaskID tezTaskId = TezTaskID.getInstance(tezVertexId, 500);
    TezTaskAttemptID tezTaskAttemptId = TezTaskAttemptID.getInstance(tezTaskId, 600);

    TaskSpec taskSpec =
        new TaskSpec(tezTaskAttemptId, "dagName", "vertexName", 10, processorDescriptor,
            inputSpecList, outputSpecList, null);

    QueryIdentifierProto queryIdentifierProto =
        QueryIdentifierProto.newBuilder().setApplicationIdString(appId.toString())
            .setAppAttemptNumber(333).setDagIndex(300).build();

    SignableVertexSpec vertexProto = Converters
        .constructSignableVertexSpec(taskSpec, queryIdentifierProto, "", "", "hiveQueryId").build();

    assertEquals("dagName", vertexProto.getDagName());
    assertEquals("vertexName", vertexProto.getVertexName());
    assertEquals("hiveQueryId", vertexProto.getHiveQueryId());
    assertEquals(appId.toString(), vertexProto.getQueryIdentifier().getApplicationIdString());
    assertEquals(tezDagId.getId(), vertexProto.getQueryIdentifier().getDagIndex());
    assertEquals(333, vertexProto.getQueryIdentifier().getAppAttemptNumber());
    assertEquals(tezVertexId.getId(), vertexProto.getVertexIndex());
    assertEquals(processorDescriptor.getClassName(),
        vertexProto.getProcessorDescriptor().getClassName());
    assertEquals(processorDescriptor.getUserPayload().getPayload(),
        vertexProto.getProcessorDescriptor().getUserPayload().getUserPayload()
            .asReadOnlyByteBuffer());
    assertEquals(2, vertexProto.getInputSpecsCount());
    assertEquals(2, vertexProto.getOutputSpecsCount());

    verifyInputSpecAndProto(inputSpec1, vertexProto.getInputSpecs(0));
    verifyInputSpecAndProto(inputSpec2, vertexProto.getInputSpecs(1));
    verifyOutputSpecAndProto(outputSpec1, vertexProto.getOutputSpecs(0));
    verifyOutputSpecAndProto(outputSpec2, vertexProto.getOutputSpecs(1));

  }

  @Test (timeout = 10000)
  public void testFragmentSpecToTaskSpec() {

    ByteBuffer procBb = ByteBuffer.allocate(4);
    procBb.putInt(0, 200);

    ByteBuffer input1Bb = ByteBuffer.allocate(4);
    input1Bb.putInt(0, 300);

    ByteBuffer output1Bb = ByteBuffer.allocate(4);
    output1Bb.putInt(0, 400);

    ApplicationId appId = ApplicationId.newInstance(1000, 100);
    TezDAGID tezDagId = TezDAGID.getInstance(appId, 300);
    TezVertexID tezVertexId = TezVertexID.getInstance(tezDagId, 400);
    TezTaskID tezTaskId = TezTaskID.getInstance(tezVertexId, 500);
    TezTaskAttemptID tezTaskAttemptId = TezTaskAttemptID.getInstance(tezTaskId, 600);

    QueryIdentifierProto queryIdentifierProto =
        QueryIdentifierProto.newBuilder().setApplicationIdString(appId.toString())
            .setAppAttemptNumber(333).setDagIndex(tezDagId.getId()).build();

    SignableVertexSpec.Builder builder = SignableVertexSpec.newBuilder();
    builder.setQueryIdentifier(queryIdentifierProto);
    builder.setHiveQueryId("hiveQueryId");
    builder.setVertexIndex(tezVertexId.getId());
    builder.setDagName("dagName");
    builder.setVertexName("vertexName");
    builder.setProcessorDescriptor(
        EntityDescriptorProto.newBuilder().setClassName("fakeProcessorName").setUserPayload(
            UserPayloadProto.newBuilder().setUserPayload(ByteString.copyFrom(procBb))));
    builder.addInputSpecs(IOSpecProto.newBuilder().setConnectedVertexName("sourceVertexName1")
        .setPhysicalEdgeCount(33).setIoDescriptor(
            EntityDescriptorProto.newBuilder().setClassName("input1ClassName").setUserPayload(
                UserPayloadProto.newBuilder().setUserPayload(ByteString.copyFrom(input1Bb)))));
    builder.addInputSpecs(IOSpecProto.newBuilder().setConnectedVertexName("sourceVertexName2")
        .setPhysicalEdgeCount(44).setIoDescriptor(
            EntityDescriptorProto.newBuilder().setClassName("input1ClassName").setUserPayload(
                UserPayloadProto.newBuilder().setUserPayload(ByteString.copyFrom(input1Bb)))));
    builder.addOutputSpecs(IOSpecProto.newBuilder().setConnectedVertexName("destVertexName1")
        .setPhysicalEdgeCount(55).setIoDescriptor(
            EntityDescriptorProto.newBuilder().setClassName("outputClassName").setUserPayload(
                UserPayloadProto.newBuilder().setUserPayload(ByteString.copyFrom(output1Bb)))));
    builder.addOutputSpecs(IOSpecProto.newBuilder().setConnectedVertexName("destVertexName2")
        .setPhysicalEdgeCount(66).setIoDescriptor(
            EntityDescriptorProto.newBuilder().setClassName("outputClassName").setUserPayload(
                UserPayloadProto.newBuilder().setUserPayload(ByteString.copyFrom(output1Bb)))));

    SignableVertexSpec vertexProto = builder.build();

    TaskSpec taskSpec = Converters.getTaskSpecfromProto(vertexProto, 0, 0, tezTaskAttemptId);

    assertEquals("dagName", taskSpec.getDAGName());
    assertEquals("vertexName", taskSpec.getVertexName());
    assertEquals(tezTaskAttemptId, taskSpec.getTaskAttemptID());
    assertEquals("fakeProcessorName", taskSpec.getProcessorDescriptor().getClassName());
    byte[] serialized = new byte[taskSpec.getProcessorDescriptor().getUserPayload().getPayload().remaining()];
    taskSpec.getProcessorDescriptor().getUserPayload().getPayload().get(serialized);
    assertArrayEquals(procBb.array(), serialized);

    assertEquals(2, taskSpec.getInputs().size());
    assertEquals(2, taskSpec.getOutputs().size());

    verifyInputSpecAndProto(taskSpec.getInputs().get(0), vertexProto.getInputSpecs(0));
    verifyInputSpecAndProto(taskSpec.getInputs().get(1), vertexProto.getInputSpecs(1));
    verifyOutputSpecAndProto(taskSpec.getOutputs().get(0), vertexProto.getOutputSpecs(0));
    verifyOutputSpecAndProto(taskSpec.getOutputs().get(1), vertexProto.getOutputSpecs(1));
  }

  private void verifyInputSpecAndProto(InputSpec inputSpec,
                                      IOSpecProto inputSpecProto) {
    assertEquals(inputSpec.getPhysicalEdgeCount(), inputSpecProto.getPhysicalEdgeCount());
    assertEquals(inputSpec.getSourceVertexName(), inputSpecProto.getConnectedVertexName());
    assertEquals(inputSpec.getInputDescriptor().getClassName(),
        inputSpecProto.getIoDescriptor().getClassName());
    assertEquals(inputSpec.getInputDescriptor().getUserPayload().getPayload(),
        inputSpecProto.getIoDescriptor().getUserPayload().getUserPayload().asReadOnlyByteBuffer());
  }

  private void verifyOutputSpecAndProto(OutputSpec outputSpec,
                                       IOSpecProto outputSpecProto) {
    assertEquals(outputSpec.getPhysicalEdgeCount(), outputSpecProto.getPhysicalEdgeCount());
    assertEquals(outputSpec.getDestinationVertexName(), outputSpecProto.getConnectedVertexName());
    assertEquals(outputSpec.getOutputDescriptor().getClassName(),
        outputSpecProto.getIoDescriptor().getClassName());
    assertEquals(outputSpec.getOutputDescriptor().getUserPayload().getPayload(),
        outputSpecProto.getIoDescriptor().getUserPayload().getUserPayload().asReadOnlyByteBuffer());
  }
}
