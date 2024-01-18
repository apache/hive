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

package org.apache.hadoop.hive.ql.exec;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.persistence.PTFRowContainer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/*
 * represents a collection of rows that is acted upon by a TableFunction or a WindowFunction.
 */
public class PTFPartition {
  protected static Logger LOG = LoggerFactory.getLogger(PTFPartition.class);

  AbstractSerDe serDe;
  StructObjectInspector inputOI;
  StructObjectInspector outputOI;
  private final PTFRowContainer<List<Object>> elems;
  protected BoundaryCache boundaryCache;
  protected PTFValueCache valueCache;

  /*
   * Used by VectorPTFGroupBatches. Currently, VectorPTFGroupBatches extends from PTFPartition in
   * order to use already implemented classes for range calculation (e.g. BoundaryScanner) on
   * vectorized codepaths. The optimal solution would be to change PTFPartition to be an interface
   * and put the used methods there, e.g. getAt(i).
   */
  protected PTFPartition() {
    elems = null;
    boundaryCache = null;
  }

  protected PTFPartition(Configuration cfg,
      AbstractSerDe serDe, StructObjectInspector inputOI,
      StructObjectInspector outputOI)
      throws HiveException {
    this(cfg, serDe, inputOI, outputOI, true);
  }
  
  protected PTFPartition(Configuration cfg,
      AbstractSerDe serDe, StructObjectInspector inputOI,
      StructObjectInspector outputOI,
      boolean createElemContainer)
      throws HiveException {
    this.serDe = serDe;
    this.inputOI = inputOI;
    this.outputOI = outputOI;
    if ( createElemContainer ) {
      int containerNumRows = HiveConf.getIntVar(cfg, ConfVars.HIVE_JOIN_CACHE_SIZE);
      elems = new PTFRowContainer<List<Object>>(containerNumRows, cfg, null);
      elems.setSerDe(serDe, outputOI);
      elems.setTableDesc(PTFRowContainer.createTableDesc(inputOI));
    } else {
      elems = null;
    }
    initBoundaryCache(cfg);
    initValueCache(cfg);
  }

  protected void initBoundaryCache(Configuration cfg) {
    int boundaryCacheSize = HiveConf.getIntVar(cfg, ConfVars.HIVE_PTF_RANGECACHE_SIZE);
    boundaryCache = boundaryCacheSize >= 1 ? new BoundaryCache(boundaryCacheSize) : null;
  }

  protected void initValueCache(Configuration cfg) {
    int valueCacheSize = HiveConf.getIntVar(cfg, ConfVars.HIVE_PTF_VALUECACHE_SIZE);
    boolean valueCacheCollectStatistics =
        HiveConf.getBoolVar(cfg, ConfVars.HIVE_PTF_VALUECACHE_COLLECT_STATISTICS);
    valueCache =
        valueCacheSize >= 1 ? new PTFValueCache(valueCacheSize, valueCacheCollectStatistics) : null;
  }

  public void reset() throws HiveException {
    elems.clearRows();
  }

  public AbstractSerDe getSerDe() {
    return serDe;
  }

  public StructObjectInspector getInputOI() {
    return inputOI;
  }

  public StructObjectInspector getOutputOI() {
    return outputOI;
  }

  public Object getAt(int i) throws HiveException
  {
    return elems.getAt(i);
  }

  public void append(Object o) throws HiveException {

    if ( elems.rowCount() == Integer.MAX_VALUE ) {
      throw new HiveException(String.format("Cannot add more than %d elements to a PTFPartition",
          Integer.MAX_VALUE));
    }

    @SuppressWarnings("unchecked")
    List<Object> l = (List<Object>)
        ObjectInspectorUtils.copyToStandardObject(o, inputOI, ObjectInspectorCopyOption.WRITABLE);
    elems.addRow(l);
  }

  public int size() {
    return elems.rowCount();
  }

  public PTFPartitionIterator<Object> iterator() throws HiveException {
    elems.first();
    return new PItr(0, size());
  }

  public PTFPartitionIterator<Object> range(int start, int end) {
    assert (start >= 0);
    assert (end <= size());
    assert (start <= end);
    return new PItr(start, end);
  }

  public PTFPartitionIterator<Object> range(int start, int end, boolean optimisedIteration) {
    return (optimisedIteration) ? new OptimisedPItr(start, end) : range(start, end);
  }

  public void close() {
    try {
      elems.close();
    } catch (HiveException e) {
      LOG.error(e.toString(), e);
    }
  }

  class PItr implements PTFPartitionIterator<Object> {
    int idx;
    final int start;
    final int end;
    final int createTimeSz;

    PItr(int start, int end) {
      this.idx = start;
      this.start = start;
      this.end = end;
      createTimeSz = PTFPartition.this.size();
    }

    @Override
    public boolean hasNext() {
      checkForComodification();
      return idx < end;
    }

    @Override
    public Object next() {
      checkForComodification();
      try {
        return PTFPartition.this.getAt(idx++);
      } catch (HiveException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    final void checkForComodification() {
      if (createTimeSz != PTFPartition.this.size()) {
        throw new ConcurrentModificationException();
      }
    }

    @Override
    public int getIndex() {
      return idx;
    }

    private Object getAt(int i) throws HiveException {
      return PTFPartition.this.getAt(i);
    }

    @Override
    public Object lead(int amt) throws HiveException {
      int i = idx + amt;
      i = i >= createTimeSz ? createTimeSz - 1 : i; // Lead on the whole partition not the iterator range
      return getAt(i);
    }

    @Override
    public Object lag(int amt) throws HiveException {
      int i = idx - amt;
      i = i < 0 ? 0 : i; // Lag on the whole partition not the iterator range
      return getAt(i);
    }

    @Override
    public Object resetToIndex(int idx) throws HiveException {
      if (idx < start || idx >= end) {
        return null;
      }
      Object o = getAt(idx);
      this.idx = idx + 1;
      return o;
    }

    @Override
    public PTFPartition getPartition() {
      return PTFPartition.this;
    }

    @Override
    public void reset() {
      idx = start;
    }

    @Override
    public long count() {
      return (end - start);
    }
  };

  /**
   * This can be used for functions with no parameters.
   * next() function in this impl does not fetch anything from rowContainer, thus saving IO.
   * It just increments the pointer to maintain iterator contract.
   */
  class OptimisedPItr extends PItr {

    OptimisedPItr(int start, int end) {
      super(start, end);
    }

    @Override
    public Object next() {
      checkForComodification();
      idx++;
      return null;
    }
  }

  /*
   * provide an Iterator on the rows in a Partition.
   * Iterator exposes the index of the next location.
   * Client can invoke lead/lag relative to the next location.
   */
  public static interface PTFPartitionIterator<T> extends Iterator<T> {
    int getIndex();

    T lead(int amt) throws HiveException;

    T lag(int amt) throws HiveException;

    /*
     * after a lead and lag call, allow Object associated with SerDe and writable associated with
     * partition to be reset
     * to the value for the current Index.
     */
    Object resetToIndex(int idx) throws HiveException;

    PTFPartition getPartition();

    void reset() throws HiveException;

    long count();
  }

  public static PTFPartition create(Configuration cfg,
      AbstractSerDe serDe,
      StructObjectInspector inputOI,
      StructObjectInspector outputOI)
      throws HiveException {
    return new PTFPartition(cfg, serDe, inputOI, outputOI);
  }
  
  public static PTFRollingPartition createRolling(Configuration cfg,
      AbstractSerDe serDe,
      StructObjectInspector inputOI,
      StructObjectInspector outputOI,
      int precedingSpan,
      int followingSpan)
      throws HiveException {
    return new PTFRollingPartition(cfg, serDe, inputOI, outputOI, precedingSpan, followingSpan);
  }

  public static StructObjectInspector setupPartitionOutputOI(AbstractSerDe serDe,
      StructObjectInspector tblFnOI) throws SerDeException {
    return (StructObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(tblFnOI,
        ObjectInspectorCopyOption.WRITABLE);
  }

  public BoundaryCache getBoundaryCache() {
    return boundaryCache;
  }

}
