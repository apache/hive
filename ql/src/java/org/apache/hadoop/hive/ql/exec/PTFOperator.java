package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.PTFPartition.PTFPartitionIterator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.PTFTranslator;
import org.apache.hadoop.hive.ql.parse.PTFTranslator.PTFDefDeserializer;
import org.apache.hadoop.hive.ql.parse.PTFTranslator.PTFTranslationInfo;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.PTFDesc.ColumnDef;
import org.apache.hadoop.hive.ql.plan.PTFDesc.PTFInputDef;
import org.apache.hadoop.hive.ql.plan.PTFDesc.PartitionDef;
import org.apache.hadoop.hive.ql.plan.PTFDesc.TableFuncDef;
import org.apache.hadoop.hive.ql.plan.PTFDesc.WhereDef;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLeadLag;
import org.apache.hadoop.hive.ql.udf.ptf.TableFunctionEvaluator;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Writable;

public class PTFOperator extends Operator<PTFDesc> implements Serializable
{

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
	protected void initializeOp(Configuration jobConf) throws HiveException
	{
		hiveConf = new HiveConf(jobConf, PTFOperator.class);
		// if the parent is ExtractOperator, this invocation is from reduce-side
		Operator<? extends OperatorDesc> parentOp = getParentOperators().get(0);
		if (parentOp instanceof ExtractOperator)
		{
			isMapOperator = false;
		}
		else
		{
			isMapOperator = true;
		}

		reconstructQueryDef(hiveConf);
    inputPart = PTFOperator.createFirstPartitionForChain(conf,
        inputObjInspectors[0], hiveConf, isMapOperator);

		// OI for FileSinkOperator is taken from select-list (reduce-side)
		// OI for ReduceSinkOperator is taken from TODO
		if (isMapOperator)
		{
			TableFuncDef tDef = PTFTranslator.getFirstTableFunction(conf);
			outputObjInspector = tDef.getRawInputOI();
		}
		else
		{
			outputObjInspector = conf.getSelectList().getOI();
		}

		setupKeysWrapper(inputObjInspectors[0]);

		super.initializeOp(jobConf);
	}

	@Override
	protected void closeOp(boolean abort) throws HiveException
	{
		super.closeOp(abort);
    if(inputPart.size() != 0){
      if (isMapOperator)
      {
        processMapFunction();
      }
      else
      {
        processInputPartition();
      }
    }
	}

	@Override
	public void processOp(Object row, int tag) throws HiveException
	{
	  if (!isMapOperator )
    {
      /*
       * checkif current row belongs to the current accumulated Partition:
       * - If not:
       *  - process the current Partition
       *  - reset input Partition
       * - set currentKey to the newKey if it is null or has changed.
       */
      newKeys.getNewKey(row, inputPart.getOI());
      boolean keysAreEqual = (currentKeys != null && newKeys != null)?
              newKeys.equals(currentKeys) : false;

      if (currentKeys != null && !keysAreEqual)
      {
        processInputPartition();
        inputPart = PTFOperator.createFirstPartitionForChain(conf, inputObjInspectors[0], hiveConf, isMapOperator);
      }

      if (currentKeys == null || !keysAreEqual)
      {
        if (currentKeys == null)
        {
          currentKeys = newKeys.copyKey();
        }
        else
        {
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
	protected void reconstructQueryDef(HiveConf hiveConf) throws HiveException
	{

	  PTFDefDeserializer qdd = new PTFDefDeserializer(hiveConf,
				inputObjInspectors[0]);
	  PTFTranslator.PTFDefWalker qdw = new PTFTranslator.PTFDefWalker(qdd);
		qdw.walk(conf);
	}

	protected void setupKeysWrapper(ObjectInspector inputOI) throws HiveException
	{
		PartitionDef pDef = PTFTranslator.getFirstTableFunction(conf).getWindow().getPartDef();
		ArrayList<ColumnDef> cols = pDef.getColumns();
		int numCols = cols.size();
		ExprNodeEvaluator[] keyFields = new ExprNodeEvaluator[numCols];
		ObjectInspector[] keyOIs = new ObjectInspector[numCols];
		ObjectInspector[] currentKeyOIs = new ObjectInspector[numCols];

		for(int i=0; i<numCols; i++)
		{
			ColumnDef cDef = cols.get(i);
			/*
			 * Why cannot we just use the ExprNodeEvaluator on the column?
			 * - because on the reduce-side it is initialized based on the rowOI of the HiveTable
			 *   and not the OI of the ExtractOp ( the parent of this Operator on the reduce-side)
			 */
			keyFields[i] = ExprNodeEvaluatorFactory.get(cDef.getExprNode());
			keyOIs[i] = keyFields[i].initialize(inputOI);
			currentKeyOIs[i] = ObjectInspectorUtils.getStandardObjectInspector(keyOIs[i], ObjectInspectorCopyOption.WRITABLE);
		}

		keyWrapperFactory = new KeyWrapperFactory(keyFields, keyOIs, currentKeyOIs);

	    newKeys = keyWrapperFactory.getKeyWrapper();
	}

	protected void processInputPartition() throws HiveException
	{
	  PTFPartition outPart = PTFOperator.executeChain(conf, inputPart);
    PTFOperator.executeSelectList(conf, outPart, this);
	}

	protected void processMapFunction() throws HiveException
	{
	  TableFuncDef tDef = PTFTranslator.getFirstTableFunction(conf);
    PTFPartition outPart = tDef.getFunction().transformRawInput(inputPart);
    PTFPartitionIterator<Object> pItr = outPart.iterator();
    while (pItr.hasNext())
    {
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
	public OperatorType getType()
	{
		return OperatorType.PTF;
	}

	 /**
   * For all the table functions to be applied to the input
   * hive table or query, push them on a stack.
   * For each table function popped out of the stack,
   * execute the function on the input partition
   * and return an output partition.
   * @param ptfDesc
   * @param part
   * @return
   * @throws HiveException
   */
  private static PTFPartition executeChain(PTFDesc ptfDesc, PTFPartition part)
      throws HiveException
  {
    Stack<TableFuncDef> fnDefs = new Stack<TableFuncDef>();
    PTFInputDef iDef = ptfDesc.getInput();
    while (true)
    {
      if (iDef instanceof TableFuncDef)
      {
        fnDefs.push((TableFuncDef) iDef);
        iDef = ((TableFuncDef) iDef).getInput();
      }
      else
      {
        break;
      }
    }

    TableFuncDef currFnDef;
    while (!fnDefs.isEmpty())
    {
      currFnDef = fnDefs.pop();
      part = currFnDef.getFunction().execute(part);
    }
    return part;
  }

  /**
   * For each row in the partition:
   * 1. evaluate the where condition if applicable.
   * 2. evaluate the value for each column retrieved
   *    from the select list
   * 3. Forward the writable value or object based on the
   *    implementation of the ForwardSink
   * @param ptfDesc
   * @param oPart
   * @param rS
   * @throws HiveException
   */
  @SuppressWarnings(
  { "rawtypes", "unchecked" })
  private static void executeSelectList(PTFDesc ptfDesc, PTFPartition oPart, PTFOperator op)
      throws HiveException
  {
    StructObjectInspector selectOI = ptfDesc.getSelectList().getOI();
    StructObjectInspector inputOI = ptfDesc.getInput().getOI();
    int numCols = selectOI.getAllStructFieldRefs().size();
    ArrayList<ColumnDef> cols = ptfDesc.getSelectList().getColumns();
    int numSelCols = cols == null ? 0 : cols.size();
    Object[] output = new Object[numCols];


    WhereDef whDef = ptfDesc.getWhere();
    boolean applyWhere = whDef != null;
    Converter whConverter = !applyWhere ? null
        : ObjectInspectorConverters
            .getConverter(
                whDef.getOI(),
                PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);
    ExprNodeEvaluator whCondEval = !applyWhere ? null : whDef
        .getExprEvaluator();

    Writable value = null;
    PTFPartitionIterator<Object> pItr = oPart.iterator();
    PTFOperator.connectLeadLagFunctionsToPartition(ptfDesc, pItr);
    while (pItr.hasNext())
    {
      int colCnt = 0;
      Object oRow = pItr.next();

      if (applyWhere)
      {
        Object whCond = null;
        whCond = whCondEval.evaluate(oRow);
        whCond = whConverter.convert(whCond);
        if (whCond == null || !((Boolean) whCond).booleanValue())
        {
          continue;
        }
      }

      /*
       * Setup the output row columns in the following order
       * - the columns in the SelectList processed by the PTF (ie the Select Exprs that have navigation expressions)
       * - the columns from the final PTF.
       */

      if ( cols != null ) {
        for (ColumnDef cDef : cols)
        {
          Object newCol = cDef.getExprEvaluator().evaluate(oRow);
          output[colCnt++] = newCol;
        }
      }

      for(; colCnt < numCols; ) {
        StructField field = inputOI.getAllStructFieldRefs().get(colCnt - numSelCols);
        output[colCnt++] = ObjectInspectorUtils.copyToStandardObject(inputOI.getStructFieldData(oRow, field),
            field.getFieldObjectInspector());
      }

      op.forward(output, op.outputObjInspector);
    }
  }

  /**
   * Create a new partition.
   * The input OI is used to evaluate rows appended to the partition.
   * The serde is determined based on whether the query has a map-phase
   * or not. The OI on the serde is used by PTFs to evaluate output of the
   * partition.
   * @param ptfDesc
   * @param oi
   * @param hiveConf
   * @return
   * @throws HiveException
   */
  public static PTFPartition createFirstPartitionForChain(PTFDesc ptfDesc, ObjectInspector oi,
      HiveConf hiveConf, boolean isMapSide) throws HiveException
  {
    TableFuncDef tabDef = PTFTranslator.getFirstTableFunction(ptfDesc);
    TableFunctionEvaluator tEval = tabDef.getFunction();
    String partClassName = tEval.getPartitionClass();
    int partMemSize = tEval.getPartitionMemSize();

    PTFPartition part = null;
    SerDe serde = tabDef.getInput().getSerde();
    part = new PTFPartition(partClassName, partMemSize, serde,
        (StructObjectInspector) oi);
    return part;

  }

  public static void connectLeadLagFunctionsToPartition(PTFDesc ptfDesc,
      PTFPartitionIterator<Object> pItr) throws HiveException
  {
    PTFTranslationInfo tInfo = ptfDesc.getTranslationInfo();
    List<ExprNodeGenericFuncDesc> llFnDescs = tInfo.getLLInfo()
        .getLeadLagExprs();
    if (llFnDescs == null) {
      return;
    }
    for (ExprNodeGenericFuncDesc llFnDesc : llFnDescs)
    {
      GenericUDFLeadLag llFn = (GenericUDFLeadLag) llFnDesc
          .getGenericUDF();
      llFn.setpItr(pItr);
    }
  }



}
