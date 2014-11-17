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

package org.apache.hadoop.hive.ql.exec.vector;

import org.apache.hadoop.hive.ql.exec.MapOperator;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.Writable;

public class VectorMapOperator extends MapOperator {

  private static final long serialVersionUID = 1L;

  @Override
  public void process(Writable value) throws HiveException {
    // A mapper can span multiple files/partitions.
    // The serializers need to be reset if the input file changed
    ExecMapperContext context = getExecContext();
    if (context != null && context.inputFileChanged()) {
      // The child operators cleanup if input file has changed
      cleanUpInputFileChanged();
    }

    // The row has been converted to comply with table schema, irrespective of partition schema.
    // So, use tblOI (and not partOI) for forwarding
    try {
      int childrenDone = 0;
      for (MapOpCtx current : currentCtxs) {
        if (!current.forward(value)) {
          childrenDone++;
        }
      }

      rowsForwarded(childrenDone, ((VectorizedRowBatch)value).size);
    } catch (Exception e) {
      throw new HiveException("Hive Runtime Error while processing row ", e);
    }
  }
}
