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
package org.apache.hadoop.hive.ql.optimizer.listbucketingpruner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.PrunerOperatorFactory;
import org.apache.hadoop.hive.ql.optimizer.pcr.PcrOpProcFactory;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Walk through top operators in tree to find all partitions.
 *
 * It should be done already in by {@link PcrOpProcFactory}
 *
 */
public class LBPartitionProcFactory extends PrunerOperatorFactory {
  static final Log LOG = LogFactory.getLog(ListBucketingPruner.class.getName());

  /**
   * Retrieve partitions for the filter. This is called only when
   * the filter follows a table scan operator.
   */
  public static class LBPRPartitionFilterPruner extends FilterPruner {

    @Override
    protected void generatePredicate(NodeProcessorCtx procCtx, FilterOperator fop,
        TableScanOperator top) throws SemanticException, UDFArgumentException {
      LBOpPartitionWalkerCtx owc = (LBOpPartitionWalkerCtx) procCtx;

      //Run partition pruner to get partitions
      ParseContext parseCtx = owc.getParseContext();
      PrunedPartitionList prunedPartList;
      try {
        String alias = (String) parseCtx.getTopOps().keySet().toArray()[0];
        prunedPartList = PartitionPruner.prune(top, parseCtx, alias);
      } catch (HiveException e) {
        // Has to use full name to make sure it does not conflict with
        // org.apache.commons.lang.StringUtils
        throw new SemanticException(e.getMessage(), e);
      }

      if (prunedPartList != null) {
        owc.setPartitions(prunedPartList);
      }

    }

  }

  public static NodeProcessor getFilterProc() {
    return new LBPRPartitionFilterPruner();
  }

  private LBPartitionProcFactory() {
    // prevent instantiation
  }

}
