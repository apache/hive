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
package org.apache.hadoop.hive.ql.udf.generic;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * GenericUDAFCollectSet
 */
@Description(name = "collect_set", value = "_FUNC_(x) - Returns a set of objects with duplicate elements eliminated")
public class GenericUDAFCollectSet extends AbstractGenericUDAFResolver {

  static final Log LOG = LogFactory.getLog(GenericUDAFCollectSet.class.getName());
  
  public GenericUDAFCollectSet() {
  }

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {

    if (parameters.length != 1) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Exactly one argument is expected.");
    }

    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "Only primitive type arguments are accepted but "
          + parameters[0].getTypeName() + " was passed as parameter 1.");
    }

    return new GenericUDAFMkSetEvaluator();
  }

  public static class GenericUDAFMkSetEvaluator extends GenericUDAFEvaluator {
    
    // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
    private PrimitiveObjectInspector inputOI;
    // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
    // of objs)
    private StandardListObjectInspector loi;
    
    private StandardListObjectInspector internalMergeOI;
    
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      super.init(m, parameters);
      // init output object inspectors
      // The output of a partial aggregation is a list
      if (m == Mode.PARTIAL1) {
        inputOI = (PrimitiveObjectInspector) parameters[0];
        return ObjectInspectorFactory
            .getStandardListObjectInspector((PrimitiveObjectInspector) ObjectInspectorUtils
                .getStandardObjectInspector(inputOI));
      } else {
        if (!(parameters[0] instanceof StandardListObjectInspector)) {
          //no map aggregation.
          inputOI = (PrimitiveObjectInspector)  ObjectInspectorUtils
          .getStandardObjectInspector(parameters[0]);
          return (StandardListObjectInspector) ObjectInspectorFactory
              .getStandardListObjectInspector(inputOI);
        } else {
          internalMergeOI = (StandardListObjectInspector) parameters[0];
          inputOI = (PrimitiveObjectInspector) internalMergeOI.getListElementObjectInspector();
          loi = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);          
          return loi;
        }
      }
    }
    
    static class MkArrayAggregationBuffer implements AggregationBuffer {
      Set<Object> container;
    }
    
    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((MkArrayAggregationBuffer) agg).container = new HashSet<Object>();
    }
    
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      MkArrayAggregationBuffer ret = new MkArrayAggregationBuffer();
      reset(ret);
      return ret;
    }

    //mapside
    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
        throws HiveException {
      assert (parameters.length == 1);
      Object p = parameters[0];

      if (p != null) {
        MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
        putIntoSet(p, myagg);
      }
    }

    //mapside
    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
      ArrayList<Object> ret = new ArrayList<Object>(myagg.container.size());
      ret.addAll(myagg.container);
      return ret;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial)
        throws HiveException {
      MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
      ArrayList<Object> partialResult = (ArrayList<Object>) internalMergeOI.getList(partial);
      for(Object i : partialResult) {
        putIntoSet(i, myagg);
      }
    }
    
    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
      ArrayList<Object> ret = new ArrayList<Object>(myagg.container.size());
      ret.addAll(myagg.container);
      return ret;
    }
    
    private void putIntoSet(Object p, MkArrayAggregationBuffer myagg) {
      Object pCopy = ObjectInspectorUtils.copyToStandardObject(p,
          this.inputOI);
      myagg.container.add(pCopy);
    }
  }
  
}
