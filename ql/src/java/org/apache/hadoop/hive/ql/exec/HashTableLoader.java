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
package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinKey;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;

/**
 * HashTableLoader is an interface used by MapJoinOperator used to load the hashtables
 * needed to process the join.
 */
public interface HashTableLoader {

  void init(ExecMapperContext context, Configuration hconf, MapJoinOperator joinOp);

  void load(MapJoinTableContainer[] mapJoinTables, MapJoinTableContainerSerDe[] mapJoinTableSerdes)
      throws HiveException;
}
