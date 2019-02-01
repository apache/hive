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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writer implementation for rel nodes that produces an output in json that is easily
 * parseable back into rel nodes.
 */
public class HiveRelWriterImpl extends RelJsonWriter {

  protected static final Logger LOG = LoggerFactory.getLogger(HiveRelWriterImpl.class);

  //~ Constructors -------------------------------------------------------------

  public HiveRelWriterImpl() {
    super();
  }

  //~ Methods ------------------------------------------------------------------

  protected void explain_(RelNode rel, List<Pair<String, Object>> values) {
    super.explain_(rel, values);
    // TODO: The following is hackish since we do not have visibility over relList
    // and we do not want to bring all the writer utilities from Calcite. It should
    // be changed once we move to new Calcite version and relList is visible for
    // subclasses.
    try {
      final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
      Field fs = RelJsonWriter.class.getDeclaredField("relList");
      fs.setAccessible(true);
      List<Object> relList = (List<Object>) fs.get(this);
      Map<String, Object> map = (Map<String, Object>) relList.get(relList.size() - 1);
      map.put("rowCount", mq.getRowCount(rel));
      if (rel.getInputs().size() == 0) {
        // This is a leaf, we will print the average row size and schema
        map.put("avgRowSize", mq.getAverageRowSize(rel));
        map.put("rowType", rel.getRowType().toString());
      }
    } catch (Exception e) {
      LOG.warn("Failed to add additional fields in json writer", e);
    }
  }

}
