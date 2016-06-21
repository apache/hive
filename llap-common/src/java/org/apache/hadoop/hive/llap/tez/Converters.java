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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.EntityDescriptorProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.GroupInputSpecProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.IOSpecProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryIdentifierProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SignableVertexSpec;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.UserPayloadProto;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.dag.api.EntityDescriptor;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.TaskContext;
import org.apache.tez.runtime.api.impl.GroupInputSpec;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;

public class Converters {

  public static TaskSpec getTaskSpecfromProto(SignableVertexSpec vectorProto,
      int fragmentNum, int attemptNum, TezTaskAttemptID attemptId) {
    TezTaskAttemptID taskAttemptID = attemptId != null ? attemptId
        : createTaskAttemptId(vectorProto.getQueryIdentifier(), vectorProto.getVertexIndex(),
        fragmentNum, attemptNum);

    ProcessorDescriptor processorDescriptor = null;
    if (vectorProto.hasProcessorDescriptor()) {
      processorDescriptor = convertProcessorDescriptorFromProto(
          vectorProto.getProcessorDescriptor());
    }

    List<InputSpec> inputSpecList = new ArrayList<InputSpec>(vectorProto.getInputSpecsCount());
    if (vectorProto.getInputSpecsCount() > 0) {
      for (IOSpecProto inputSpecProto : vectorProto.getInputSpecsList()) {
        inputSpecList.add(getInputSpecFromProto(inputSpecProto));
      }
    }

    List<OutputSpec> outputSpecList =
        new ArrayList<OutputSpec>(vectorProto.getOutputSpecsCount());
    if (vectorProto.getOutputSpecsCount() > 0) {
      for (IOSpecProto outputSpecProto : vectorProto.getOutputSpecsList()) {
        outputSpecList.add(getOutputSpecFromProto(outputSpecProto));
      }
    }

    List<GroupInputSpec> groupInputSpecs =
        new ArrayList<GroupInputSpec>(vectorProto.getGroupedInputSpecsCount());
    if (vectorProto.getGroupedInputSpecsCount() > 0) {
      for (GroupInputSpecProto groupInputSpecProto : vectorProto.getGroupedInputSpecsList()) {
        groupInputSpecs.add(getGroupInputSpecFromProto(groupInputSpecProto));
      }
    }

    TaskSpec taskSpec =
        new TaskSpec(taskAttemptID, vectorProto.getDagName(), vectorProto.getVertexName(),
            vectorProto.getVertexParallelism(), processorDescriptor, inputSpecList,
            outputSpecList, groupInputSpecs);
    return taskSpec;
  }

  public static TezTaskAttemptID createTaskAttemptId(
      QueryIdentifierProto queryIdProto, int vertexIndex, int fragmentNum, int attemptNum) {
    // Come ride the API roller-coaster!
    return TezTaskAttemptID.getInstance(
            TezTaskID.getInstance(
                TezVertexID.getInstance(
                    TezDAGID.getInstance(
                        ConverterUtils.toApplicationId(
                            queryIdProto.getApplicationIdString()),
                        queryIdProto.getDagIndex()),
                    vertexIndex),
                fragmentNum),
            attemptNum);
  }

  public static TezTaskAttemptID createTaskAttemptId(TaskContext ctx) {
    // Come ride the API roller-coaster #2! The best part is that ctx has TezTaskAttemptID inside.
    return TezTaskAttemptID.getInstance(
            TezTaskID.getInstance(
                TezVertexID.getInstance(
                    TezDAGID.getInstance(
                        ctx.getApplicationId(),
                    ctx.getDagIdentifier()),
                ctx.getTaskVertexIndex()),
            ctx.getTaskIndex()),
        ctx.getTaskAttemptNumber());
  }

  public static SignableVertexSpec.Builder constructSignableVertexSpec(TaskSpec taskSpec,
                                                                       QueryIdentifierProto queryIdentifierProto,
                                                                       String tokenIdentifier,
                                                                       String user,
                                                                       String hiveQueryIdString) {
    TezTaskAttemptID tId = taskSpec.getTaskAttemptID();

    SignableVertexSpec.Builder builder = SignableVertexSpec.newBuilder();
    builder.setQueryIdentifier(queryIdentifierProto);
    builder.setHiveQueryId(hiveQueryIdString);
    builder.setVertexIndex(tId.getTaskID().getVertexID().getId());
    builder.setDagName(taskSpec.getDAGName());
    builder.setVertexName(taskSpec.getVertexName());
    builder.setVertexParallelism(taskSpec.getVertexParallelism());
    builder.setTokenIdentifier(tokenIdentifier);
    builder.setUser(user);

    if (taskSpec.getProcessorDescriptor() != null) {
      builder.setProcessorDescriptor(
          convertToProto(taskSpec.getProcessorDescriptor()));
    }

    if (taskSpec.getInputs() != null && !taskSpec.getInputs().isEmpty()) {
      for (InputSpec inputSpec : taskSpec.getInputs()) {
        builder.addInputSpecs(convertInputSpecToProto(inputSpec));
      }
    }

    if (taskSpec.getOutputs() != null && !taskSpec.getOutputs().isEmpty()) {
      for (OutputSpec outputSpec : taskSpec.getOutputs()) {
        builder.addOutputSpecs(convertOutputSpecToProto(outputSpec));
      }
    }

    if (taskSpec.getGroupInputs() != null && !taskSpec.getGroupInputs().isEmpty()) {
      for (GroupInputSpec groupInputSpec : taskSpec.getGroupInputs()) {
        builder.addGroupedInputSpecs(convertGroupInputSpecToProto(groupInputSpec));

      }
    }
    return builder;
  }

  private static ProcessorDescriptor convertProcessorDescriptorFromProto(
      EntityDescriptorProto proto) {
    String className = proto.getClassName();
    UserPayload payload = convertPayloadFromProto(proto);
    ProcessorDescriptor pd = ProcessorDescriptor.create(className);
    setUserPayload(pd, payload);
    return pd;
  }

  private static EntityDescriptorProto convertToProto(
      EntityDescriptor<?> descriptor) {
    EntityDescriptorProto.Builder builder = EntityDescriptorProto
        .newBuilder();
    builder.setClassName(descriptor.getClassName());

    UserPayload userPayload = descriptor.getUserPayload();
    if (userPayload != null) {
      UserPayloadProto.Builder payloadBuilder = UserPayloadProto.newBuilder();
      if (userPayload.hasPayload()) {
        payloadBuilder.setUserPayload(ByteString.copyFrom(userPayload.getPayload()));
        payloadBuilder.setVersion(userPayload.getVersion());
      }
      builder.setUserPayload(payloadBuilder.build());
    }
    if (descriptor.getHistoryText() != null) {
      try {
        builder.setHistoryText(TezCommonUtils.compressByteArrayToByteString(
            descriptor.getHistoryText().getBytes("UTF-8")));
      } catch (IOException e) {
        throw new TezUncheckedException(e);
      }
    }
    return builder.build();
  }

  private static InputSpec getInputSpecFromProto(IOSpecProto inputSpecProto) {
    InputDescriptor inputDescriptor = null;
    if (inputSpecProto.hasIoDescriptor()) {
      inputDescriptor =
          convertInputDescriptorFromProto(inputSpecProto.getIoDescriptor());
    }
    InputSpec inputSpec = new InputSpec(inputSpecProto.getConnectedVertexName(), inputDescriptor,
        inputSpecProto.getPhysicalEdgeCount());
    return inputSpec;
  }

  private static InputDescriptor convertInputDescriptorFromProto(
      EntityDescriptorProto proto) {
    String className = proto.getClassName();
    UserPayload payload = convertPayloadFromProto(proto);
    InputDescriptor id = InputDescriptor.create(className);
    setUserPayload(id, payload);
    return id;
  }

  private static OutputDescriptor convertOutputDescriptorFromProto(
      EntityDescriptorProto proto) {
    String className = proto.getClassName();
    UserPayload payload = convertPayloadFromProto(proto);
    OutputDescriptor od = OutputDescriptor.create(className);
    setUserPayload(od, payload);
    return od;
  }

  private static IOSpecProto convertInputSpecToProto(InputSpec inputSpec) {
    IOSpecProto.Builder builder = IOSpecProto.newBuilder();
    if (inputSpec.getSourceVertexName() != null) {
      builder.setConnectedVertexName(inputSpec.getSourceVertexName());
    }
    if (inputSpec.getInputDescriptor() != null) {
      builder.setIoDescriptor(convertToProto(inputSpec.getInputDescriptor()));
    }
    builder.setPhysicalEdgeCount(inputSpec.getPhysicalEdgeCount());
    return builder.build();
  }

  private static OutputSpec getOutputSpecFromProto(IOSpecProto outputSpecProto) {
    OutputDescriptor outputDescriptor = null;
    if (outputSpecProto.hasIoDescriptor()) {
      outputDescriptor =
          convertOutputDescriptorFromProto(outputSpecProto.getIoDescriptor());
    }
    OutputSpec outputSpec =
        new OutputSpec(outputSpecProto.getConnectedVertexName(), outputDescriptor,
            outputSpecProto.getPhysicalEdgeCount());
    return outputSpec;
  }

  public static IOSpecProto convertOutputSpecToProto(OutputSpec outputSpec) {
    IOSpecProto.Builder builder = IOSpecProto.newBuilder();
    if (outputSpec.getDestinationVertexName() != null) {
      builder.setConnectedVertexName(outputSpec.getDestinationVertexName());
    }
    if (outputSpec.getOutputDescriptor() != null) {
      builder.setIoDescriptor(convertToProto(outputSpec.getOutputDescriptor()));
    }
    builder.setPhysicalEdgeCount(outputSpec.getPhysicalEdgeCount());
    return builder.build();
  }

  private static GroupInputSpec getGroupInputSpecFromProto(GroupInputSpecProto groupInputSpecProto) {
    GroupInputSpec groupSpec = new GroupInputSpec(groupInputSpecProto.getGroupName(),
        groupInputSpecProto.getGroupVerticesList(),
        convertInputDescriptorFromProto(groupInputSpecProto.getMergedInputDescriptor()));
    return groupSpec;
  }

  private static GroupInputSpecProto convertGroupInputSpecToProto(GroupInputSpec groupInputSpec) {
    GroupInputSpecProto.Builder builder = GroupInputSpecProto.newBuilder();
    builder.setGroupName(groupInputSpec.getGroupName());
    builder.addAllGroupVertices(groupInputSpec.getGroupVertices());
    builder.setMergedInputDescriptor(convertToProto(groupInputSpec.getMergedInputDescriptor()));
    return builder.build();
  }


  private static void setUserPayload(EntityDescriptor<?> entity, UserPayload payload) {
    if (payload != null) {
      entity.setUserPayload(payload);
    }
  }

  private static UserPayload convertPayloadFromProto(
      EntityDescriptorProto proto) {
    UserPayload userPayload = null;
    if (proto.hasUserPayload()) {
      if (proto.getUserPayload().hasUserPayload()) {
        userPayload =
            UserPayload.create(proto.getUserPayload().getUserPayload().asReadOnlyByteBuffer(), proto.getUserPayload().getVersion());
      } else {
        userPayload = UserPayload.create(null);
      }
    }
    return userPayload;
  }

  public static SourceStateProto fromVertexState(VertexState state) {
    switch (state) {
      case SUCCEEDED:
        return SourceStateProto.S_SUCCEEDED;
      case RUNNING:
        return SourceStateProto.S_RUNNING;
      default:
        throw new RuntimeException("Unexpected state: " + state);
    }
  }
}
