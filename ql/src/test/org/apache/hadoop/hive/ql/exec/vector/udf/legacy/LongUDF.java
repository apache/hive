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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.LongWritable;

/* A UDF like one a user would create, implementing the UDF interface.
 * This is to be used to test the vectorized UDF adaptor for legacy-style UDFs.
 */

@Description(
   name = "longudf",
   value = "_FUNC_(arg) - returns arg + 1000",
   extended = "Example:\n" +
   "  > SELECT longudf(eno) FROM employee;\n"
   )

public class LongUDF extends UDF {
 public LongWritable evaluate(LongWritable i) {
   if (i == null) {
     return null;
   }
   return new LongWritable(i.get() + 1000);
 }
}