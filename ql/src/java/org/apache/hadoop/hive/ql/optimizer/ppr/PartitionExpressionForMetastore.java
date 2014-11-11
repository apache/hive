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

package org.apache.hadoop.hive.ql.optimizer.ppr;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

/**
 * The basic implementation of PartitionExpressionProxy that uses ql package classes.
 */
public class PartitionExpressionForMetastore implements PartitionExpressionProxy {
  private static final Log LOG = LogFactory.getLog(PartitionExpressionForMetastore.class);

  @Override
  public String convertExprToFilter(byte[] exprBytes) throws MetaException {
    return deserializeExpr(exprBytes).getExprString();
  }

  @Override
  public boolean filterPartitionsByExpr(List<String> partColumnNames,
      List<PrimitiveTypeInfo> partColumnTypeInfos, byte[] exprBytes,
      String defaultPartitionName, List<String> partitionNames) throws MetaException {
    ExprNodeGenericFuncDesc expr = deserializeExpr(exprBytes);
    try {
      long startTime = System.nanoTime(), len = partitionNames.size();
      boolean result = PartitionPruner.prunePartitionNames(
          partColumnNames, partColumnTypeInfos, expr, defaultPartitionName, partitionNames);
      double timeMs = (System.nanoTime() - startTime) / 1000000.0;
      LOG.debug("Pruning " + len + " partition names took " + timeMs + "ms");
      return result;
    } catch (HiveException ex) {
      LOG.error("Failed to apply the expression", ex);
      throw new MetaException(ex.getMessage());
    }
  }

  private ExprNodeGenericFuncDesc deserializeExpr(byte[] exprBytes) throws MetaException {
    ExprNodeGenericFuncDesc expr = null;
    try {
      expr = Utilities.deserializeExpressionFromKryo(exprBytes);
    } catch (Exception ex) {
      LOG.error("Failed to deserialize the expression", ex);
      throw new MetaException(ex.getMessage());
    }
    if (expr == null) {
      throw new MetaException("Failed to deserialize expression - ExprNodeDesc not present");
    }
    return expr;
  }
}
