package org.apache.hadoop.hive.ql.udf.generic;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.WindowFunctionDescription;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

@WindowFunctionDescription
(
		description = @Description(
								name = "percent_rank",
								value = "_FUNC_(x) PERCENT_RANK is similar to CUME_DIST, but it uses rank values rather " +
									"than row counts in its numerator. PERCENT_RANK of a row is calculated as: " +
									"(rank of row in its partition - 1) / (number of rows in the partition - 1)"
								),
		supportsWindow = false,
		pivotResult = true
)
public class GenericUDAFPercentRank extends GenericUDAFRank
{
	static final Log LOG = LogFactory.getLog(GenericUDAFPercentRank.class.getName());

	@Override
  protected GenericUDAFRankEvaluator createEvaluator()
	{
		return new GenericUDAFPercentRankEvaluator();
	}

	public static class GenericUDAFPercentRankEvaluator extends GenericUDAFRankEvaluator
	{
		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException
		{
			super.init(m, parameters);
			return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
		}

		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException
		{
			ArrayList<IntWritable> ranks =  ((RankBuffer) agg).rowNums;
			double sz = ranks.size();
			if ( sz > 1 ) {
        sz = sz - 1;
      }
			ArrayList<DoubleWritable> pranks = new ArrayList<DoubleWritable>(ranks.size());

			for(IntWritable i : ranks)
			{
				double pr = ((double)i.get() - 1)/sz;
				pranks.add(new DoubleWritable(pr));
			}

			return pranks;
		}
	}
}
