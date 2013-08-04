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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.WindowFunctionDescription;

@WindowFunctionDescription
(
		description = @Description(
								name = "dense_rank",
								value = "_FUNC_(x) The difference between RANK and DENSE_RANK is that DENSE_RANK leaves no " +
										"gaps in ranking sequence when there are ties. That is, if you were " +
										"ranking a competition using DENSE_RANK and had three people tie for " +
										"second place, you would say that all three were in second place and " +
										"that the next person came in third."
								),
		supportsWindow = false,
		pivotResult = true,
		rankingFunction = true,
		impliesOrder = true
)
public class GenericUDAFDenseRank extends GenericUDAFRank
{
	static final Log LOG = LogFactory.getLog(GenericUDAFDenseRank.class.getName());

	@Override
  protected GenericUDAFRankEvaluator createEvaluator()
	{
		return new GenericUDAFDenseRankEvaluator();
	}

	public static class GenericUDAFDenseRankEvaluator extends GenericUDAFRankEvaluator
	{
		/*
		 * Called when the value in the partition has changed. Update the currentRank
		 */
		@Override
    protected void nextRank(RankBuffer rb)
		{
			rb.currentRank++;
		}
	}
}

