/**
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

package org.apache.hive.builtins;

import java.util.HashMap;
import java.util.Map;

import org.apache.hive.pdk.HivePdkUnitTest;
import org.apache.hive.pdk.HivePdkUnitTests;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Aggregate all maps into a single map. If there are multiple values for
 * the same key, result can contain any of those values.
 * Because the mappers must keep all of the data in memory, if your data is
 * non-trivially large you should set hive.map.aggr=false to ensure that
 * UNION_MAP is only executed in the reduce phase.
 */
@HivePdkUnitTests(
  setup = "",
  cleanup = "",
  cases = {
    @HivePdkUnitTest(
      query = "SELECT size(UNION_MAP(MAP(sepal_width, sepal_length))) "
      +"FROM iris",
      result = "23")
  })
@Description(
  name = "union_map",
  value = "_FUNC_(col) - aggregate given maps into a single map",
  extended = "Aggregate maps, returns as a HashMap.")
public class UDAFUnionMap extends AbstractGenericUDAFResolver {
  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {

    // Next two validation calls are dependent on HIVE-2524, so
    // leave them commented out for now.
    //
    // new LengthEquals(1).check(parameters.length);
    // new IsMap().check(parameters[0], 0);

    return new Evaluator();
  }

  public static class State implements AggregationBuffer {
    HashMap<Object, Object> map = new HashMap<Object, Object>();
  }

  public static class Evaluator extends GenericUDAFEvaluator {
    ObjectInspector inputOI;
    MapObjectInspector internalMergeOI;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);
      if (m == Mode.COMPLETE || m == Mode.PARTIAL1) {
        inputOI = (MapObjectInspector) parameters[0];
      } else {
        internalMergeOI = (MapObjectInspector) parameters[0];
      }
      return ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new State();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] input) throws HiveException {
      if (input[0] != null) {
        State state = (State) agg;
        state.map.putAll((Map<?,?>)ObjectInspectorUtils.copyToStandardObject(input[0], inputOI));
      }
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial != null) {
        State state = (State) agg;
        Map<?,?> pset = (Map<?,?>)ObjectInspectorUtils.copyToStandardObject(partial, internalMergeOI);
        state.map.putAll(pset);
      }
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((State) agg).map.clear();
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      return ((State) agg).map;
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      return ((State) agg).map;
    }
  }
}
