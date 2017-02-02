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

package org.apache.hadoop.hive.ql.udf.ptf;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.PTFOperator;
import org.apache.hadoop.hive.ql.exec.PTFPartition;
import org.apache.hadoop.hive.ql.exec.PTFPartition.PTFPartitionIterator;
import org.apache.hadoop.hive.ql.exec.PTFUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.ptf.PartitionedTableFunctionDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/*
 * Interface Design:
 * A TableFunction provides 2 interfaces of execution 'Batch' and 'Streaming'.
 * - In Batch mode the contract is Partition in - Partition Out
 * - In Streaming mode the contract is a stream of processRow calls - each of which may return 0 or more rows.
 * 
 * A Partition is not just a batch of rows, it enables more than a single iteration of
 * the i/p data: multiple passes, arbitrary access of input rows, relative navigation between
 * rows(for e.g. lead/lag fns). Most PTFs will work in batch mode.
 * 
 * The Streaming mode gives up on the capabilities of Partitions for the benefit of smaller footprint,
 * and faster processing. Window Function processing is an e.g. of this: when there are only Ranking
 * functions each row needs to be accessed once in the order it is provided; hence there is no need 
 * to hold all input rows in a Partition. The 'pattern' is: any time you want to only enhance/enrich 
 * an Input Row Streaming mode is the right choice. This is the fundamental difference between Ranking
 * fns and UDAFs: Ranking functions keep the original data intact whereas UDAF only return aggregate
 * information.
 * 
 * Finally we have provided a 'mixed' mode where a non Streaming TableFunction can provide its output
 * as an Iterator. As far as we can tell, this is a special case for Windowing handling. If Windowing
 * is the only or last TableFunction in a chain, it makes no sense to collect the output rows into a 
 * output Partition. We justify the pollution of the api by the observation that Windowing is a very 
 * common use case.
 * 
 */

/**
 * Based on Hive {@link GenericUDAFEvaluator}. Break up the responsibility of the old AbstractTableFunction
 * class into a Resolver and Evaluator.
 * <p>
 * The Evaluator also holds onto the {@link TableFunctionDef}. This provides information
 * about the arguments to the function, the shape of the Input partition and the Partitioning details.
 * The Evaluator is responsible for providing the 2 execute methods:
 * <ol>
 * <li><b>execute:</b> which is invoked after the input is partitioned; the contract
 * is, it is given an input Partition and must return an output Partition. The shape of the output
 * Partition is obtained from the getOutputOI call.
 * <li><b>transformRawInput:</b> In the case where this function indicates that it will transform the raw input
 * before it is fed through the partitioning mechanics, this function is called. Again the contract is
 * t is given an input Partition and must return an Partition. The shape of the output Partition is
 * obtained from getRawInputOI() call.
 * </ol>
 *
 */
public abstract class TableFunctionEvaluator {
  /*
   * how is this different from the OutputShape set on the TableDef.
   * This is the OI of the object coming out of the PTF.
   * It is put in an output Partition whose Serde is usually LazyBinarySerde.
   * So the next PTF (or Operator) in the chain gets a LazyBinaryStruct.
   */
  transient protected StructObjectInspector OI;
  /*
   * same comment as OI applies here.
   */
  transient protected StructObjectInspector rawInputOI;
  protected PartitionedTableFunctionDef tableDef;
  protected PTFDesc ptfDesc;
  boolean transformsRawInput;
  transient protected PTFPartition outputPartition;
  transient protected boolean canAcceptInputAsStream;

  public StructObjectInspector getOutputOI() {
    return OI;
  }

  protected void setOutputOI(StructObjectInspector outputOI) {
    OI = outputOI;
  }

  public PartitionedTableFunctionDef getTableDef() {
    return tableDef;
  }

  public void setTableDef(PartitionedTableFunctionDef tDef) {
    this.tableDef = tDef;
  }

  protected PTFDesc getQueryDef() {
    return ptfDesc;
  }

  protected void setQueryDef(PTFDesc ptfDesc) {
    this.ptfDesc = ptfDesc;
  }

  public StructObjectInspector getRawInputOI() {
    return rawInputOI;
  }

  protected void setRawInputOI(StructObjectInspector rawInputOI) {
    this.rawInputOI = rawInputOI;
  }

  public boolean isTransformsRawInput() {
    return transformsRawInput;
  }

  public void setTransformsRawInput(boolean transformsRawInput) {
    this.transformsRawInput = transformsRawInput;
  }

  public PTFPartition execute(PTFPartition iPart)
      throws HiveException {
    if ( ptfDesc.isMapSide() ) {
      return transformRawInput(iPart);
    }
    PTFPartitionIterator<Object> pItr = iPart.iterator();
    PTFOperator.connectLeadLagFunctionsToPartition(ptfDesc.getLlInfo(), pItr);

    if ( outputPartition == null ) {
      outputPartition = PTFPartition.create(ptfDesc.getCfg(),
          tableDef.getOutputShape().getSerde(),
          OI, tableDef.getOutputShape().getOI());
    } else {
      outputPartition.reset();
    }

    execute(pItr, outputPartition);
    return outputPartition;
  }

  protected abstract void execute(PTFPartitionIterator<Object> pItr, PTFPartition oPart) throws HiveException;

  protected PTFPartition transformRawInput(PTFPartition iPart) throws HiveException {
    if (!isTransformsRawInput()) {
      throw new HiveException(String.format("Internal Error: mapExecute called on function (%s)that has no Map Phase", tableDef.getName()));
    }
    return _transformRawInput(iPart);
  }

  protected PTFPartition _transformRawInput(PTFPartition iPart) throws HiveException {
    return null;
  }


  /*
   * A TableFunction may be able to provide its Output as an Iterator.
   * In case it can then for Map-side processing and for the last PTF in a Reduce-side chain
   * we can forward rows one by one. This will save the time/space to populate and read an Output
   * Partition.
   */
  public boolean canIterateOutput() {
    return false;
  }

  public Iterator<Object> iterator(PTFPartitionIterator<Object> pItr) throws HiveException {
    
    if ( ptfDesc.isMapSide() ) {
      return transformRawInputIterator(pItr);
    }
    
    if (!canIterateOutput()) {
      throw new HiveException(
          "Internal error: iterator called on a PTF that cannot provide its output as an Iterator");
    }
    throw new HiveException(String.format(
        "Internal error: PTF %s, provides no iterator method",
        getClass().getName()));
  }
  
  protected Iterator<Object> transformRawInputIterator(PTFPartitionIterator<Object> pItr) throws HiveException {
    if (!canIterateOutput()) {
      throw new HiveException(
          "Internal error: iterator called on a PTF that cannot provide its output as an Iterator");
    }
    throw new HiveException(String.format(
        "Internal error: PTF %s, provides no iterator method",
        getClass().getName()));
  }
  
  /*
   * A TableFunction may be able to accept its input as a stream.
   * In this case the contract is:
   * - startPartition must be invoked to give the PTF a chance to initialize stream processing.
   * - each input row is passed in via a processRow(or processRows) invocation. processRow 
   *   can return 0 or more o/p rows.
   * - finishPartition is invoked to give the PTF a chance to finish processing and return any 
   *   remaining o/p rows.
   */
  public boolean canAcceptInputAsStream() {
    return canAcceptInputAsStream;
  }

  public void initializeStreaming(Configuration cfg,
      StructObjectInspector inputOI, boolean isMapSide) throws HiveException {
    canAcceptInputAsStream = false;
  }

  public void startPartition() throws HiveException {
    if (!canAcceptInputAsStream() ) {
      throw new HiveException(String.format(
          "Internal error: PTF %s, doesn't support Streaming",
          getClass().getName()));
    }
  }
  
  public List<Object> processRow(Object row) throws HiveException {
    if (!canAcceptInputAsStream() ) {
      throw new HiveException(String.format(
          "Internal error: PTF %s, doesn't support Streaming",
          getClass().getName()));
    }
    
    return null;
  }
  
  public List<Object> finishPartition() throws HiveException {
    if (!canAcceptInputAsStream() ) {
      throw new HiveException(String.format(
          "Internal error: PTF %s, doesn't support Streaming",
          getClass().getName()));
    }
    return null;
  }

  public void close() {
    if (outputPartition != null) {
      outputPartition.close();
    }
    outputPartition = null;
  }
}

