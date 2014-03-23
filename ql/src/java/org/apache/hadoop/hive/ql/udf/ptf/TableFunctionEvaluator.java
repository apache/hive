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

import org.apache.hadoop.hive.ql.exec.PTFOperator;
import org.apache.hadoop.hive.ql.exec.PTFPartition;
import org.apache.hadoop.hive.ql.exec.PTFPartition.PTFPartitionIterator;
import org.apache.hadoop.hive.ql.exec.PTFUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.ptf.PartitionedTableFunctionDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * Based on Hive {@link GenericUDAFEvaluator}. Break up the responsibility of the old AsbtractTableFunction
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
   * how is this different from the OutpuShape set on the TableDef.
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

  static {
    //TODO is this a bug? The field is not named outputOI it is named OI
    PTFUtils.makeTransient(TableFunctionEvaluator.class, "outputOI", "rawInputOI");
  }

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
    PTFPartitionIterator<Object> pItr = iPart.iterator();
    PTFOperator.connectLeadLagFunctionsToPartition(ptfDesc, pItr);

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

  public PTFPartition transformRawInput(PTFPartition iPart) throws HiveException {
    if (!isTransformsRawInput()) {
      throw new HiveException(String.format("Internal Error: mapExecute called on function (%s)that has no Map Phase", tableDef.getName()));
    }
    return _transformRawInput(iPart);
  }

  protected PTFPartition _transformRawInput(PTFPartition iPart) throws HiveException {
    return null;
  }

  public void close() {
    if (outputPartition != null) {
      outputPartition.close();
    }
    outputPartition = null;
  }
}

