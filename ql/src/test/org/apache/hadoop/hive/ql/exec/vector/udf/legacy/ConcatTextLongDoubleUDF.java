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

package org.apache.hadoop.hive.ql.exec.vector.udf.legacy;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.Text;

@Description(
		name = "testudf",
		value = "_FUNC_(str) - combines arguments to output string",
		extended = "Example:\n" +
		"  > SELECT testudf(name, dob, salary) FROM employee;\n" +
		"  Jack"
		)

/* This is a test function that takes three different kinds
 * of arguments, for use to verify vectorized UDF invocation.
 */
public class ConcatTextLongDoubleUDF extends UDF {
	public Text evaluate(Text s, Long i, Double d) {

		if (s == null
				|| i == null
				|| d == null) {
			return null;
		}
		StringBuilder sb = new StringBuilder();
		sb.append(s.toString());
		sb.append(":");
		sb.append(i);
		sb.append(":");
		sb.append(d);
		return new Text(sb.toString());
	}
}
