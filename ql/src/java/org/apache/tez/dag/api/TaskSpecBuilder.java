package org.apache.tez.dag.api;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;

// Proxy class within the tez.api package to access package private methods.
public class TaskSpecBuilder {

  public TaskSpec constructTaskSpec(DAG dag, String vertexName, int numSplits) {
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

    TaskSpec taskSpec = TaskSpec
        .createBaseTaskSpec(dag.getName(), vertexName, numSplits, processorDescriptor, inputSpecs,
            outputSpecs, null);

    return taskSpec;
  }

}
