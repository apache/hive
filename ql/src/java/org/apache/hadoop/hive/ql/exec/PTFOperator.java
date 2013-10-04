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

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.PTFPartition.PTFPartitionIterator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.PTFDeserializer;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.plan.ptf.PTFExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.PTFInputDef;
import org.apache.hadoop.hive.ql.plan.ptf.PartitionDef;
import org.apache.hadoop.hive.ql.plan.ptf.PartitionedTableFunctionDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLeadLag;
import org.apache.hadoop.hive.ql.udf.ptf.TableFunctionEvaluator;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class PTFOperator extends Operator<PTFDesc> implements Serializable {

	private static final long serialVersionUID = 1L;
	PTFPartition inputPart;
	boolean isMapOperator;

	transient KeyWrapperFactory keyWrapperFactory;
	protected transient KeyWrapper currentKeys;
	protected transient KeyWrapper newKeys;
	transient HiveConf hiveConf;


	/*
	 * 1. Find out if the operator is invoked at Map-Side or Reduce-side
	 * 2. Get the deserialized QueryDef
	 * 3. Reconstruct the transient variables in QueryDef
	 * 4. Create input partition to store rows coming from previous operator
	 */
	@Override
	protected void initializeOp(Configuration jobConf) throws HiveException {
		hiveConf = new HiveConf(jobConf, PTFOperator.class);
		// if the parent is ExtractOperator, this invocation is from reduce-side
		Operator<? extends OperatorDesc> parentOp = getParentOperators().get(0);
		isMapOperator = conf.isMapSide();

		reconstructQueryDef(hiveConf);
    inputPart = createFirstPartitionForChain(
        inputObjInspectors[0], hiveConf, isMapOperator);

		if (isMapOperator) {
			PartitionedTableFunctionDef tDef = conf.getStartOfChain();
			outputObjInspector = tDef.getRawInputShape().getOI();
		} else {
			outputObjInspector = conf.getFuncDef().getOutputShape().getOI();
		}

		setupKeysWrapper(inputObjInspectors[0]);

		super.initializeOp(jobConf);
	}

	@Override
	protected void closeOp(boolean abort) throws HiveException {
		super.closeOp(abort);
    if(inputPart.size() != 0){
      if (isMapOperator) {
        processMapFunction();
      } else {
        processInputPartition();
      }
    }
    inputPart.close();
	}

	@Override
	public void processOp(Object row, int tag) throws HiveException
	{
	  if (!isMapOperator ) {
      /*
       * checkif current row belongs to the current accumulated Partition:
       * - If not:
       *  - process the current Partition
       *  - reset input Partition
       * - set currentKey to the newKey if it is null or has changed.
       */
      newKeys.getNewKey(row, inputPart.getInputOI());
      boolean keysAreEqual = (currentKeys != null && newKeys != null)?
              newKeys.equals(currentKeys) : false;

      if (currentKeys != null && !keysAreEqual) {
        processInputPartition();
        inputPart.reset();
      }

      if (currentKeys == null || !keysAreEqual) {
        if (currentKeys == null) {
          currentKeys = newKeys.copyKey();
        } else {
          currentKeys.copyKey(newKeys);
        }
      }
    }

    // add row to current Partition.
    inputPart.append(row);
	}

	/**
	 * Initialize the visitor to use the QueryDefDeserializer Use the order
	 * defined in QueryDefWalker to visit the QueryDef
	 *
	 * @param hiveConf
	 * @throws HiveException
	 */
	protected void reconstructQueryDef(HiveConf hiveConf) throws HiveException {

	  PTFDeserializer dS =
	      new PTFDeserializer(conf, (StructObjectInspector)inputObjInspectors[0], hiveConf);
	  dS.initializePTFChain(conf.getFuncDef());
	}

	protected void setupKeysWrapper(ObjectInspector inputOI) throws HiveException {
		PartitionDef pDef = conf.getStartOfChain().getPartition();
		List<PTFExpressionDef> exprs = pDef.getExpressions();
		int numExprs = exprs.size();
		ExprNodeEvaluator[] keyFields = new ExprNodeEvaluator[numExprs];
		ObjectInspector[] keyOIs = new ObjectInspector[numExprs];
		ObjectInspector[] currentKeyOIs = new ObjectInspector[numExprs];

		for(int i=0; i<numExprs; i++) {
		  PTFExpressionDef exprDef = exprs.get(i);
			/*
			 * Why cannot we just use the ExprNodeEvaluator on the column?
			 * - because on the reduce-side it is initialized based on the rowOI of the HiveTable
			 *   and not the OI of the ExtractOp ( the parent of this Operator on the reduce-side)
			 */
			keyFields[i] = ExprNodeEvaluatorFactory.get(exprDef.getExprNode());
			keyOIs[i] = keyFields[i].initialize(inputOI);
			currentKeyOIs[i] =
			    ObjectInspectorUtils.getStandardObjectInspector(keyOIs[i],
			        ObjectInspectorCopyOption.WRITABLE);
		}

		keyWrapperFactory = new KeyWrapperFactory(keyFields, keyOIs, currentKeyOIs);
	  newKeys = keyWrapperFactory.getKeyWrapper();
	}

	protected void processInputPartition() throws HiveException {
	  PTFPartition outPart = executeChain(inputPart);
	  PTFPartitionIterator<Object> pItr = outPart.iterator();
    while (pItr.hasNext()) {
      Object oRow = pItr.next();
      forward(oRow, outputObjInspector);
    }
	}

	protected void processMapFunction() throws HiveException {
	  PartitionedTableFunctionDef tDef = conf.getStartOfChain();
    PTFPartition outPart = tDef.getTFunction().transformRawInput(inputPart);
    PTFPartitionIterator<Object> pItr = outPart.iterator();
    while (pItr.hasNext()) {
      Object oRow = pItr.next();
      forward(oRow, outputObjInspector);
    }
	}

	/**
	 * @return the name of the operator
	 */
  @Override
  public String getName() {
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "PTF";
  }


	@Override
	public OperatorType getType() {
		return OperatorType.PTF;
	}

	 /**
   * For all the table functions to be applied to the input
   * hive table or query, push them on a stack.
   * For each table function popped out of the stack,
   * execute the function on the input partition
   * and return an output partition.
   * @param part
   * @return
   * @throws HiveException
   */
  private PTFPartition executeChain(PTFPartition part)
      throws HiveException {
    Deque<PartitionedTableFunctionDef> fnDefs = new ArrayDeque<PartitionedTableFunctionDef>();
    PTFInputDef iDef = conf.getFuncDef();

    while (iDef instanceof PartitionedTableFunctionDef) {
      fnDefs.push((PartitionedTableFunctionDef) iDef);
      iDef = ((PartitionedTableFunctionDef) iDef).getInput();
    }

    PartitionedTableFunctionDef currFnDef;
    while (!fnDefs.isEmpty()) {
      currFnDef = fnDefs.pop();
      part = currFnDef.getTFunction().execute(part);
    }
    return part;
  }


  /**
   * Create a new Partition.
   * A partition has 2 OIs: the OI for the rows being put in and the OI for the rows
   * coming out. You specify the output OI by giving the Serde to use to Serialize.
   * Typically these 2 OIs are the same; but not always. For the
   * first PTF in a chain the OI of the incoming rows is dictated by the Parent Op
   * to this PTFOp. The output OI from the Partition is typically LazyBinaryStruct, but
   * not always. In the case of Noop/NoopMap we keep the Strcuture the same as
   * what is given to us.
   * <p>
   * The Partition we want to create here is for feeding the First table function in the chain.
   * So for map-side processing use the Serde from the output Shape its InputDef.
   * For reduce-side processing use the Serde from its RawInputShape(the shape
   * after map-side processing).
   * @param oi
   * @param hiveConf
   * @param isMapSide
   * @return
   * @throws HiveException
   */
  public PTFPartition createFirstPartitionForChain(ObjectInspector oi,
      HiveConf hiveConf, boolean isMapSide) throws HiveException {
    PartitionedTableFunctionDef tabDef = conf.getStartOfChain();
    TableFunctionEvaluator tEval = tabDef.getTFunction();

    PTFPartition part = null;
    SerDe serde = isMapSide ? tabDef.getInput().getOutputShape().getSerde() :
      tabDef.getRawInputShape().getSerde();
    StructObjectInspector outputOI = isMapSide ? tabDef.getInput().getOutputShape().getOI() :
      tabDef.getRawInputShape().getOI();
    part = PTFPartition.create(conf.getCfg(),
        serde,
        (StructObjectInspector) oi,
        outputOI);

    return part;

  }

  public static void connectLeadLagFunctionsToPartition(PTFDesc ptfDesc,
      PTFPartitionIterator<Object> pItr) throws HiveException {
    List<ExprNodeGenericFuncDesc> llFnDescs = ptfDesc.getLlInfo().getLeadLagExprs();
    if (llFnDescs == null) {
      return;
    }
    for (ExprNodeGenericFuncDesc llFnDesc : llFnDescs) {
      GenericUDFLeadLag llFn = (GenericUDFLeadLag) llFnDesc
          .getGenericUDF();
      llFn.setpItr(pItr);
    }
  }

}
