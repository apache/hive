package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.PTFPartition.PTFPartitionIterator;
import org.apache.hadoop.hive.ql.exec.PTFUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

public abstract class GenericUDFLeadLag extends GenericUDF
{
	transient ExprNodeEvaluator exprEvaluator;
	transient PTFPartitionIterator<Object> pItr;
	ObjectInspector firstArgOI;

	private PrimitiveObjectInspector amtOI;

	static{
		PTFUtils.makeTransient(GenericUDFLeadLag.class, "exprEvaluator");
		PTFUtils.makeTransient(GenericUDFLeadLag.class, "pItr");
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException
	{
		DeferredObject amt = arguments[1];
		int intAmt = 0;
		try
		{
			intAmt = PrimitiveObjectInspectorUtils.getInt(amt.get(), amtOI);
		}
		catch (NullPointerException e)
		{
			intAmt = Integer.MAX_VALUE;
		}
		catch (NumberFormatException e)
		{
			intAmt = Integer.MAX_VALUE;
		}

		int idx = pItr.getIndex() - 1;
		try
		{
			Object row = getRow(intAmt);
			Object ret = exprEvaluator.evaluate(row);
			ret = ObjectInspectorUtils.copyToStandardObject(ret, firstArgOI, ObjectInspectorCopyOption.WRITABLE);
			return ret;
		}
		finally
		{
			Object currRow = pItr.resetToIndex(idx);
			// reevaluate expression on current Row, to trigger the Lazy object
			// caches to be reset to the current row.
			exprEvaluator.evaluate(currRow);
		}

	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException
	{
		// index has to be a primitive
		if (arguments[1] instanceof PrimitiveObjectInspector)
		{
			amtOI = (PrimitiveObjectInspector) arguments[1];
		}
		else
		{
			throw new UDFArgumentTypeException(1,
					"Primitive Type is expected but "
							+ arguments[1].getTypeName() + "\" is found");
		}

		firstArgOI = arguments[0];
		return ObjectInspectorUtils.getStandardObjectInspector(firstArgOI,
				ObjectInspectorCopyOption.WRITABLE);
	}



	public ExprNodeEvaluator getExprEvaluator()
	{
		return exprEvaluator;
	}

	public void setExprEvaluator(ExprNodeEvaluator exprEvaluator)
	{
		this.exprEvaluator = exprEvaluator;
	}

	public PTFPartitionIterator<Object> getpItr()
	{
		return pItr;
	}

	public void setpItr(PTFPartitionIterator<Object> pItr)
	{
		this.pItr = pItr;
	}

	@Override
	public String getDisplayString(String[] children)
	{
		assert (children.length == 2);
		StringBuilder sb = new StringBuilder();
		sb.append(_getFnName());
		sb.append("(");
		sb.append(children[0]);
		sb.append(", ");
		sb.append(children[1]);
		sb.append(")");
		return sb.toString();
	}

	protected abstract String _getFnName();

	protected abstract Object getRow(int amt);

	public static class GenericUDFLead extends GenericUDFLeadLag
	{

		@Override
		protected String _getFnName()
		{
			return "lead";
		}

		@Override
		protected Object getRow(int amt)
		{
			return pItr.lead(amt - 1);
		}

	}

	public static class GenericUDFLag extends GenericUDFLeadLag
	{
		@Override
		protected String _getFnName()
		{
			return "lag";
		}

		@Override
		protected Object getRow(int amt)
		{
			return pItr.lag(amt + 1);
		}

	}

}
