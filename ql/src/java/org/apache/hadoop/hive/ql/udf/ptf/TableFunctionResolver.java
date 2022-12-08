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

package org.apache.hadoop.hive.ql.udf.ptf;

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.ptf.PartitionedTableFunctionDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * Based on Hive {@link GenericUDAFResolver}. Break up the responsibility of the
 * old AbstractTableFunction class into a Resolver and Evaluator.
 * The Resolver is responsible for:
 * <ol>
 * <li> setting up the {@link tableFunctionEvaluator}
 * <li> Setting up the The raw and output ObjectInspectors of the Evaluator.
 * <li> The Evaluator also holds onto the {@link TableFunctionDef}. This provides information
 * about the arguments to the function, the shape of the Input partition and the Partitioning details.
 * </ol>
 * The Resolver for a function is obtained from the {@link FunctionRegistry}. The Resolver is initialized
 * by the following 4 step process:
 * <ol>
 * <li> The initialize method is called; which is passed the {@link PTFDesc} and the {@link TableFunctionDef}.
 * <li> The resolver is then asked to setup the Raw ObjectInspector. This is only required if the Function reshapes
 * the raw input.
 * <li> Once the Resolver has had a chance to compute the shape of the Raw Input that is fed to the partitioning
 * machinery; the translator sets up the partitioning details on the tableFuncDef.
 * <li> finally the resolver is asked to setup the output ObjectInspector.
 * </ol>
 */
@SuppressWarnings("deprecation")
public abstract class TableFunctionResolver {
  TableFunctionEvaluator evaluator;
  PTFDesc ptfDesc;

  /*
   * - called during translation.
   * - invokes createEvaluator which must be implemented by a subclass
   * - sets up the evaluator with references to the TableDef, PartitionClass, PartitionMemsize and
   *   the transformsRawInput boolean.
   */
  public void initialize(HiveConf cfg, PTFDesc ptfDesc, PartitionedTableFunctionDef tDef)
      throws SemanticException {
    this.ptfDesc = ptfDesc;

    evaluator = createEvaluator(ptfDesc, tDef);
    evaluator.setTransformsRawInput(transformsRawInput());
    evaluator.setTableDef(tDef);
    evaluator.setQueryDef(ptfDesc);
  }

  /*
   * called during deserialization of a QueryDef during runtime.
   */
  public void initialize(PTFDesc ptfDesc, PartitionedTableFunctionDef tDef, TableFunctionEvaluator evaluator)
      throws HiveException {
    this.evaluator = evaluator;
    this.ptfDesc = ptfDesc;
    evaluator.setTableDef(tDef);
    evaluator.setQueryDef(ptfDesc);
  }

  public TableFunctionEvaluator getEvaluator() {
    return evaluator;
  }

  /*
   * - a subclass must provide this method.
   * - this method is invoked during translation and also when the Operator is initialized during runtime.
   * - a subclass must use this call to setup the shape of its output.
   * - subsequent to this call, a call to getOutputOI call on the {@link TableFunctionEvaluator} must return the OI
   * of the output of this function.
   */
  public abstract void setupOutputOI() throws SemanticException;

  /*
   * A PTF Function must provide the 'external' names of the columns in its Output.
   *
   */
  public abstract List<String> getOutputColumnNames() throws SemanticException;


  /**
   * This method is invoked during runtime(during deserialization of theQueryDef).
   * At this point the TableFunction can assume that the {@link ExprNodeDesc Expression Nodes}
   * exist for all the Def (ArgDef, ColumnDef, WindowDef..). It is the responsibility of
   * the TableFunction to construct the {@link ExprNodeEvaluator evaluators} and setup the OI.
   *
   * @param tblFuncDef
   * @param ptfDesc
   * @throws HiveException
   */
  public abstract void initializeOutputOI() throws HiveException;

  /*
   * - Called on functions that transform the raw input.
   * - this method is invoked during translation and also when the Operator is initialized during runtime.
   * - a subclass must use this call to setup the shape of the raw input, that is fed to the partitioning mechanics.
   * - subsequent to this call, a call to getRawInputOI call on the {@link TableFunctionEvaluator} must return the OI
   *   of the output of this function.
   */
  public void setupRawInputOI() throws SemanticException {
    if (!transformsRawInput()) {
      return;
    }
    throw new SemanticException(
        "Function has map phase, must extend setupMapOI");
  }

  /*
   * A PTF Function must provide the 'external' names of the columns in the transformed Raw Input.
   *
   */
  public List<String> getRawInputColumnNames() throws SemanticException {
    if (!transformsRawInput()) {
      return null;
    }
    throw new SemanticException(
        "Function transforms Raw Input; must extend getRawColumnInputNames");
  }

  /*
   * Same responsibility as initializeOI, but for the RawInput.
   */
  public void initializeRawInputOI() throws HiveException {
    if (!transformsRawInput()) {
      return;
    }
    throw new HiveException(
        "Function has map phase, must extend initializeRawInputOI");
  }

  /*
   * callback method used by subclasses to set the RawInputOI on the Evaluator.
   */
  protected void setRawInputOI(StructObjectInspector rawInputOI) {
    evaluator.setRawInputOI(rawInputOI);
  }

  /*
   * callback method used by subclasses to set the OutputOI on the Evaluator.
   */
  protected void setOutputOI(StructObjectInspector outputOI) {
    evaluator.setOutputOI(outputOI);
  }

  public PTFDesc getPtfDesc() {
    return ptfDesc;
  }

  /*
   * This is used during translation to decide if the internalName -> alias mapping from the Input to the PTF is carried
   * forward when building the Output RR for this PTF.
   * This is used by internal PTFs: NOOP, WindowingTableFunction to make names in its input available in the Output.
   * In general this should be false; and the names used for the Output Columns must be provided by the PTF Writer in the
   * function getOutputNames.
   */
  public boolean carryForwardNames() {
    return false;
  }

  /*
   * a subclass must indicate whether it will transform the raw input before it is fed through the
   * partitioning mechanics.
   */
  public abstract boolean transformsRawInput();

  /*
   * a subclass must provide the {@link TableFunctionEvaluator} instance.
   */
  protected abstract TableFunctionEvaluator createEvaluator(PTFDesc ptfDesc, PartitionedTableFunctionDef tDef);

  /**
   * Provide referenced columns names to be used in partition function
   *
   * @return null for unknown (will get all columns from table including virtual columns) 
   * @throws SemanticException
   */
  public List<String> getReferencedColumns() throws SemanticException {
    return null;
  }
}
