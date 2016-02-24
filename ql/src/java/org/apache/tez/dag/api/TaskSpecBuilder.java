package org.apache.tez.dag.api;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;

// Proxy class within the tez.api package to access package private methods.
public class TaskSpecBuilder {

  public TaskSpec constructTaskSpec(DAG dag, String vertexName, int numSplits, ApplicationId appId) {
    Vertex vertex = dag.getVertex(vertexName);
    ProcessorDescriptor processorDescriptor = vertex.getProcessorDescriptor();
    List<RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>> inputs =
        vertex.getInputs();
    List<RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor>> outputs =
        vertex.getOutputs();

    // TODO RSHACK - for now these must be of size 1.
    Preconditions.checkState(inputs.size() == 1);
    Preconditions.checkState(outputs.size() == 1);

    List<InputSpec> inputSpecs = new ArrayList<>();
    for (RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor> input : inputs) {
      InputSpec inputSpec = new InputSpec(input.getName(), input.getIODescriptor(), 1);
      inputSpecs.add(inputSpec);
    }

    List<OutputSpec> outputSpecs = new ArrayList<>();
    for (RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor> output : outputs) {
      OutputSpec outputSpec = new OutputSpec(output.getName(), output.getIODescriptor(), 1);
      outputSpecs.add(outputSpec);
    }

    TezDAGID dagId = TezDAGID.getInstance(appId, 0);
    TezVertexID vertexId = TezVertexID.getInstance(dagId, 0);
    TezTaskID taskId = TezTaskID.getInstance(vertexId, 0);
    TezTaskAttemptID taskAttemptId = TezTaskAttemptID.getInstance(taskId, 0);
    return new TaskSpec(taskAttemptId, dag.getName(), vertexName, numSplits, processorDescriptor, inputSpecs, outputSpecs, null);
  }

  public EventMetaData getDestingationMetaData(Vertex vertex) {
    List<RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>> inputs =
        vertex.getInputs();
    Preconditions.checkState(inputs.size() == 1);
    String inputName = inputs.get(0).getName();
    EventMetaData destMeta =
        new EventMetaData(EventMetaData.EventProducerConsumerType.INPUT, vertex.getName(),
            inputName, null);
    return destMeta;
  }

}
