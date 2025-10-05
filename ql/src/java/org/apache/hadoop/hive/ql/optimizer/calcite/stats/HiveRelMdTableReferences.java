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
package org.apache.hadoop.hive.ql.optimizer.calcite.stats;

import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.util.BuiltInMethod;

import java.util.Set;

public class HiveRelMdTableReferences implements MetadataHandler<BuiltInMetadata.TableReferences> {

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(new HiveRelMdTableReferences(),
          BuiltInMethod.TABLE_REFERENCES.method);

  @Override
  public MetadataDef<BuiltInMetadata.TableReferences> getDef() {
    return BuiltInMetadata.TableReferences.DEF;
  }

  public Set<RexTableInputRef.RelTableRef> getTableReferences(SingleRel r, RelMetadataQuery mq) {
    return mq.getTableReferences(r.getInput());
  }
}
