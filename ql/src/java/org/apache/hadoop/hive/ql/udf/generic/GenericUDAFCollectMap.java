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

package org.apache.hadoop.hive.ql.udf.generic;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

@Description(name = "collect_map", value = "_FUNC_(key, value) - Returns a map of the given key-value columns")
public class GenericUDAFCollectMap extends AbstractGenericUDAFResolver {
  private static void validateType(ObjectInspector inspector, int index) throws UDFArgumentTypeException {
    switch (inspector.getCategory()) {
      case PRIMITIVE:
      case STRUCT:
      case MAP:
      case LIST:
        break;
      default:
        throw new UDFArgumentTypeException(index,
            "Only primitive, struct, list or map type arguments are accepted but "
                + inspector.getTypeName() + " was passed as parameter" + (index + 1));
    }
  }

  @Override
  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
    final ObjectInspector[] parameters = info.getParameterObjectInspectors();
    if (parameters.length != 2) {
      throw new UDFArgumentTypeException(parameters.length - 1, "Exactly two arguments are expected.");
    }

    validateType(parameters[0], 0);
    validateType(parameters[1], 1);

    return new GenericUDAFCollectMapEvaluator();
  }

  private static final class GenericUDAFCollectMapEvaluator extends GenericUDAFEvaluator {
    private ObjectInspector keyInspector;
    private ObjectInspector valueInspector;

    private MapObjectInspector internalMergeOI;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);

      switch (m) {
        case PARTIAL1:
        case COMPLETE:
          // original -> partial or full aggregation
          keyInspector = parameters[0];
          valueInspector = parameters[1];
          return ObjectInspectorFactory.getStandardMapObjectInspector(
              ObjectInspectorUtils.getStandardObjectInspector(keyInspector),
              ObjectInspectorUtils.getStandardObjectInspector(valueInspector)
          );
        case PARTIAL2:
        case FINAL:
          // partial aggregation -> partial or final aggregation
          internalMergeOI = (MapObjectInspector) parameters[0];
          keyInspector = internalMergeOI.getMapKeyObjectInspector();
          valueInspector = internalMergeOI.getMapValueObjectInspector();
          return ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
        default:
          throw new SemanticException("Unexpected mode was chosen: " + m);
      }
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() {
      return new CollectMapAggregationBuffer();
    }

    @Override
    public void reset(AggregationBuffer agg) {
      ((CollectMapAggregationBuffer) agg).reset();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) {
      assert parameters.length == 2;
      final Object key = parameters[0];
      final Object value = parameters[1];
      put(agg, key, value);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) {
      return ((CollectMapAggregationBuffer) agg).getCopiedMap();
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) {
      final Map<?, ?> partialResult = internalMergeOI.getMap(partial);
      if (partialResult != null) {
        partialResult.forEach((key, value) -> put(agg, key, value));
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) {
      return ((CollectMapAggregationBuffer) agg).getCopiedMap();
    }

    private void put(AggregationBuffer buffer, Object key, Object value) {
      if (key == null) {
        return;
      }
      final Object copiedKey = ObjectInspectorUtils.copyToStandardObject(key, keyInspector);
      final Object copiedValue = ObjectInspectorUtils.copyToStandardObject(value, valueInspector);
      ((CollectMapAggregationBuffer) buffer).put(copiedKey, copiedValue);
    }

    private static class CollectMapAggregationBuffer extends AbstractAggregationBuffer {
      private Map<Object, Object> map = new LinkedHashMap<>();

      private void put(Object key, Object value) {
        map.put(key, value);
      }

      private Map<Object, Object> getCopiedMap() {
        return new LinkedHashMap<>(map);
      }

      private void reset() {
        // We never use HashMap#clear here. The time complexity of HashMap#clear is O(N), and N is the size of the
        // internal hash table. The size of the internal hash table doesn't shrink. So, reusing the map can cause
        // performance issues when aggregation keys are skewed.
        // In most case, we expect the number of entries in the map per aggregation key is fairly stable. So, we give
        // the same initial capacity as the previous aggregation so that many resizing is less likely to happen.
        map = new LinkedHashMap<>(map.size());
      }
    }
  }
}
