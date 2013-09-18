package org.apache.hadoop.hive.ql.udf.generic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;

import com.esotericsoftware.minlog.Log;

public class GenericUDAFMkCollectionEvaluator extends GenericUDAFEvaluator {

  enum BufferType { SET, LIST }

  // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
  private PrimitiveObjectInspector inputOI;
  // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
  // of objs)
  private transient StandardListObjectInspector loi;

  private transient StandardListObjectInspector internalMergeOI;

  private BufferType bufferType;

  //needed by kyro
  public GenericUDAFMkCollectionEvaluator(){

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


  class MkArrayAggregationBuffer extends AbstractAggregationBuffer {

    private Collection<Object> container;

    public MkArrayAggregationBuffer() {
      if (bufferType == BufferType.LIST){
        container = new ArrayList<Object>();
      } else if(bufferType == BufferType.SET){
        container = new HashSet<Object>();
      } else {
        throw new RuntimeException("Buffer type unknown");
      }
    }
  }

  @Override
  public void reset(AggregationBuffer agg) throws HiveException {
    ((MkArrayAggregationBuffer) agg).container.clear();
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
    List<Object> partialResult = (ArrayList<Object>) internalMergeOI.getList(partial);
    for(Object i : partialResult) {
      putIntoCollection(i, myagg);
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
