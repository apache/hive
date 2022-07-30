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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;

public class GenericUDAFMkCollectionEvaluator extends GenericUDAFEvaluator
    implements Serializable {

  private static final long serialVersionUID = 1l;

  enum BufferType { SET, LIST }

  // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
  private transient ObjectInspector inputOI;
  // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
  // of objs)
  private transient StandardListObjectInspector loi;

  private transient ListObjectInspector internalMergeOI;

  private BufferType bufferType;

  //needed by kyro
  public GenericUDAFMkCollectionEvaluator() {
  }

  public GenericUDAFMkCollectionEvaluator(BufferType bufferType){
    this.bufferType = bufferType;
  }

  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters)
      throws HiveException {
    super.init(m, parameters);
    // init output object inspectors
    // The output of a partial aggregation is a list
    if (m == Mode.PARTIAL1) {
      inputOI = parameters[0];
      return ObjectInspectorFactory.getStandardListObjectInspector(
          ObjectInspectorUtils.getStandardObjectInspector(inputOI));
    } else {
      if (!(parameters[0] instanceof ListObjectInspector)) {
        //no map aggregation.
        inputOI = ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);
        return ObjectInspectorFactory.getStandardListObjectInspector(inputOI);
      } else {
        internalMergeOI = (ListObjectInspector) parameters[0];
        inputOI = internalMergeOI.getListElementObjectInspector();
        loi = (StandardListObjectInspector)
            ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
        return loi;
      }
    }
  }


  class MkArrayAggregationBuffer extends AbstractAggregationBuffer {

    private Collection<Object> container;

    public MkArrayAggregationBuffer() {
      if (bufferType == BufferType.LIST){
        container = new ArrayList<Object>();
      } else if(bufferType == BufferType.SET){
        container = new LinkedHashSet<Object>();
      } else {
        throw new RuntimeException("Buffer type unknown");
      }
    }

    private void reset() {
      if (bufferType == BufferType.LIST) {
        container.clear();
      } else if (bufferType == BufferType.SET) {
        // Don't reuse a container because HashSet#clear can be very slow. The operation takes O(N)
        // and N is the capacity of the internal hash table. The internal capacity grows based on
        // the number of elements and it never shrinks. Thus, HashSet#clear takes O(N) every time
        // once a skewed key appears.
        // In order to avoid too many resizing in average cases, we set the initial capacity to the
        // number of elements of the previous aggregation.
        container = new LinkedHashSet<>(container.size());
      } else {
        throw new RuntimeException("Buffer type unknown");
      }
    }
  }

  @Override
  public void reset(AggregationBuffer agg) throws HiveException {
    ((MkArrayAggregationBuffer) agg).reset();
  }

  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    MkArrayAggregationBuffer ret = new MkArrayAggregationBuffer();
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
      putIntoCollection(p, myagg);
    }
  }

  //mapside
  @Override
  public Object terminatePartial(AggregationBuffer agg) throws HiveException {
    MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
    List<Object> ret = new ArrayList<Object>(myagg.container.size());
    ret.addAll(myagg.container);
    return ret;
  }

  @Override
  public void merge(AggregationBuffer agg, Object partial)
      throws HiveException {
    MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
    List<Object> partialResult = (List<Object>) internalMergeOI.getList(partial);
    if (partialResult != null) {
      for(Object i : partialResult) {
        putIntoCollection(i, myagg);
      }
    }
  }

  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
    List<Object> ret = new ArrayList<Object>(myagg.container.size());
    ret.addAll(myagg.container);
    return ret;
  }

  private void putIntoCollection(Object p, MkArrayAggregationBuffer myagg) {
    Object pCopy = ObjectInspectorUtils.copyToStandardObject(p,  this.inputOI);
    myagg.container.add(pCopy);
  }

  public BufferType getBufferType() {
    return bufferType;
  }

  public void setBufferType(BufferType bufferType) {
    this.bufferType = bufferType;
  }

}
