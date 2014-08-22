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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
@Description(
    name = "lag",
    value = "LAG  (scalar_expression [,offset] [,default]) OVER ([query_partition_clause] order_by_clause); "
        + "The LAG function is used to access data from a previous row.",
    extended = "Example:\n "
    + "select p1.p_mfgr, p1.p_name, p1.p_size,\n"
    + " p1.p_size - lag(p1.p_size,1,p1.p_size) over( distribute by p1.p_mfgr sort by p1.p_name) as deltaSz\n"
    + " from part p1 join part p2 on p1.p_partkey = p2.p_partkey")

@UDFType(impliesOrder = true)
public class GenericUDFLag extends GenericUDFLeadLag {
  @Override
  protected String _getFnName() {
    return "lag";
  }

  @Override
  protected int getIndex(int amt) {
    return pItr.getIndex() - 1 - amt;
  }

  @Override
  protected Object getRow(int amt) throws HiveException {
    return pItr.lag(amt + 1);
  }

}
