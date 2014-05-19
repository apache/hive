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

package org.apache.hadoop.hive.ql.udf.ptf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.ptf.PartitionedTableFunctionDef;

public class NoopWithMapStreaming extends NoopWithMap {
  List<Object> rows;
  
  NoopWithMapStreaming() {
    rows = new ArrayList<Object>();
  }
  
  public boolean canAcceptInputAsStream() {
    return true;
  } 
  
  public List<Object> processRow(Object row) throws HiveException {
    if (!canAcceptInputAsStream() ) {
      throw new HiveException(String.format(
          "Internal error: PTF %s, doesn't support Streaming",
          getClass().getName()));
    }
    rows.clear();
    rows.add(row);
    return rows;
  }
  
  public static class NoopWithMapStreamingResolver extends NoopWithMapResolver {

    @Override
    protected TableFunctionEvaluator createEvaluator(PTFDesc ptfDesc, PartitionedTableFunctionDef tDef) {
      return new NoopStreaming();
    }
  }
}
