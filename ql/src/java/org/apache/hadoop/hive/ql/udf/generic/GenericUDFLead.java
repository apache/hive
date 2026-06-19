/*
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
    name = "lead",
    value = "LEAD (scalar_expression [,offset] [,default]) OVER ([query_partition_clause] order_by_clause); "
        + "The LEAD function is used to return data from the next row. ",
    extended = "Example:\n "
    + "select p_name, p_retailprice, lead(p_retailprice) over() as l1,\n"
    + " lag(p_retailprice) over() as l2\n"
    + " from part\n"
    + " where p_retailprice = 1173.15")


@UDFType(impliesOrder = true)
public class GenericUDFLead extends GenericUDFLeadLag {

  @Override
  protected String _getFnName() {
    return "lead";
  }

  @Override
  protected int getIndex(int amt) {
    return pItr.getIndex() - 1 + amt;
  }

  @Override
  protected Object getRow(int amt) throws HiveException {
    return pItr.lead(amt - 1);
  }

}
