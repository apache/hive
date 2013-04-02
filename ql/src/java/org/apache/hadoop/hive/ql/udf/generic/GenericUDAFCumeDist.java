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
								name = "cume_dist",
								value = "_FUNC_(x) - The CUME_DIST function (defined as the inverse of percentile in some " +
									"statistical books) computes the position of a specified value relative to a set of values. " +
									"To compute the CUME_DIST of a value x in a set S of size N, you use the formula: " +
									"CUME_DIST(x) =  number of values in S coming before " +
									"   and including x in the specified order/ N"
								),
		supportsWindow = false,
		pivotResult = true
)
public class GenericUDAFCumeDist extends GenericUDAFRank
{

	static final Log LOG = LogFactory.getLog(GenericUDAFCumeDist.class.getName());

	@Override
  protected GenericUDAFRankEvaluator createEvaluator()
	{
		return new GenericUDAFCumeDistEvaluator();
	}

	public static class GenericUDAFCumeDistEvaluator extends GenericUDAFRankEvaluator
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
			ArrayList<DoubleWritable> pranks = new ArrayList<DoubleWritable>(ranks.size());

			for(IntWritable i : ranks)
			{
				double pr = ((double)i.get())/sz;
				pranks.add(new DoubleWritable(pr));
			}

			return pranks;
		}
	}
}
