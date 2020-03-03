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

package org.apache.hadoop.hive.ql.plan.impala.node;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.impala.ImpalaBasicAnalyzer;
import org.apache.impala.analysis.BaseTableRef;
import org.apache.impala.analysis.Path;
import org.apache.impala.analysis.TableRef;

/**
 * The ImpalaBaseTableRef derived class allows us the ability to
 * override the alias naming of the BaseTableRef
 */
public class ImpalaBaseTableRef extends BaseTableRef {

  public ImpalaBaseTableRef(TableRef tableRef, Path resolvedPath,
      ImpalaBasicAnalyzer basicAnalyzer) throws HiveException {
    super(tableRef, resolvedPath);
    // Impala's table uniqueAlias is within the scope of each Analyzer.
    // Since Impala uses a separate Analyzer instance for each query block
    // it can maintain the uniqueness.  However, since the FENG planner
    // uses a single ImpalaBasicAnalyzer for entire query and there are no
    // longer separate query blocks (they have already been unnested), it needs
    // to make the alias globally unique.
    Preconditions.checkState(aliases_.length > 0);
    aliases_[0] = basicAnalyzer.getUniqueTableAlias(getUniqueAlias());
  }

}
