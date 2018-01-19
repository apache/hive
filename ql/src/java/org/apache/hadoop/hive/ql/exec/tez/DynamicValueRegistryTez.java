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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.DynamicValueRegistry;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.RuntimeValuesInfo;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.DynamicValue.NoDynamicValuesException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamicValueRegistryTez implements DynamicValueRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicValueRegistryTez.class);

  public static class RegistryConfTez extends RegistryConf {
    public Configuration conf;
    public BaseWork baseWork;
    public ProcessorContext processorContext;
    public Map<String, LogicalInput> inputs;

    public RegistryConfTez(Configuration conf, BaseWork baseWork,
        ProcessorContext processorContext, Map<String, LogicalInput> inputs) {
      super();
      this.conf = conf;
      this.baseWork = baseWork;
      this.processorContext = processorContext;
      this.inputs = inputs;
    }
  }

  protected Map<String, Object> values = Collections.synchronizedMap(new HashMap<String, Object>());

  public DynamicValueRegistryTez() {
  }

  @Override
  public Object getValue(String key) {
    if (!values.containsKey(key)) {
      throw new NoDynamicValuesException("Value does not exist in registry: " + key);
    }
    return values.get(key);
  }

  protected void setValue(String key, Object value) {
    values.put(key, value);
  }

  @Override
  public void init(RegistryConf conf) throws Exception {
    RegistryConfTez rct = (RegistryConfTez) conf;

    for (String inputSourceName : rct.baseWork.getInputSourceToRuntimeValuesInfo().keySet()) {
      LOG.info("Runtime value source: " + inputSourceName);

      LogicalInput runtimeValueInput = rct.inputs.get(inputSourceName);
      RuntimeValuesInfo runtimeValuesInfo = rct.baseWork.getInputSourceToRuntimeValuesInfo().get(inputSourceName);

      // Setup deserializer/obj inspectors for the incoming data source
      Deserializer deserializer = ReflectionUtils.newInstance(runtimeValuesInfo.getTableDesc().getDeserializerClass(), null);
      deserializer.initialize(rct.conf, runtimeValuesInfo.getTableDesc().getProperties());
      ObjectInspector inspector = deserializer.getObjectInspector();

      // Set up col expressions for the dynamic values using this input
      List<ExprNodeEvaluator> colExprEvaluators = new ArrayList<ExprNodeEvaluator>();
      for (ExprNodeDesc expr : runtimeValuesInfo.getColExprs()) {
        ExprNodeEvaluator exprEval = ExprNodeEvaluatorFactory.get(expr, null);
        exprEval.initialize(inspector);
        colExprEvaluators.add(exprEval);
      }

      runtimeValueInput.start();
      List<Input> inputList = new ArrayList<Input>();
      inputList.add(runtimeValueInput);
      rct.processorContext.waitForAllInputsReady(inputList);

      KeyValueReader kvReader = (KeyValueReader) runtimeValueInput.getReader();
      long rowCount = 0;
      while (kvReader.next()) {
        Object row = deserializer.deserialize((Writable) kvReader.getCurrentValue());
        rowCount++;
        for (int colIdx = 0; colIdx < colExprEvaluators.size(); ++colIdx) {
          // Read each expression and save it to the value registry
          ExprNodeEvaluator eval = colExprEvaluators.get(colIdx);
          Object val = eval.evaluate(row);
          setValue(runtimeValuesInfo.getDynamicValueIDs().get(colIdx), val);
        }
      }
      // For now, expecting a single row (min/max, aggregated bloom filter), or no rows
      if (rowCount == 0) {
        LOG.debug("No input rows from " + inputSourceName + ", filling dynamic values with nulls");
        for (int colIdx = 0; colIdx < colExprEvaluators.size(); ++colIdx) {
          ExprNodeEvaluator eval = colExprEvaluators.get(colIdx);
          setValue(runtimeValuesInfo.getDynamicValueIDs().get(colIdx), null);
        }
      } else if (rowCount > 1) {
        throw new IllegalStateException("Expected 0 or 1 rows from " + inputSourceName + ", got " + rowCount);
      }
    }
  }
}
