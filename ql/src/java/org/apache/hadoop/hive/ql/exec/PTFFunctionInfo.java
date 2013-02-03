package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.exec.PartitionTableFunctionDescription;
import org.apache.hadoop.hive.ql.udf.ptf.TableFunctionResolver;

class PTFFunctionInfo
{
	String displayName;
	Class<? extends TableFunctionResolver>  functionResolver;
	boolean isInternal;

	public PTFFunctionInfo(String displayName, Class<? extends TableFunctionResolver> tFnCls)
	{
		super();
		this.displayName = displayName;
		this.functionResolver = tFnCls;
		isInternal = false;
		PartitionTableFunctionDescription def = functionResolver.getAnnotation(PartitionTableFunctionDescription.class);
		if ( def != null)
		{
			isInternal = def.isInternal();
		}
	}

	public String getDisplayName()
	{
		return displayName;
	}

	public Class<? extends TableFunctionResolver> getFunctionResolver()
	{
		return functionResolver;
	}

	public boolean isInternal()
	{
		return isInternal;
	}

}