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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelJsonReader;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.HiveSqlOperatorTable;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import static org.apache.calcite.sql.util.SqlOperatorTables.chain;

/**
 * Reads a JSON plan and converts it to a Hive relational expression (RelNode).
 */
@InterfaceStability.Evolving
public class HiveRelJsonReader {
  private final RelOptCluster cluster;

  public HiveRelJsonReader(RelOptCluster cluster) {
    this.cluster = Objects.requireNonNull(cluster);
  }

  public RelNode readFile(Path path) throws IOException {
    return readJson(Files.readString(path, Charset.defaultCharset()));
  }

  public RelNode readJson(String json) throws IOException {
    HiveConf conf = cluster.getPlanner().getContext().unwrap(HiveConf.class);
    if (conf == null) {
      conf = new HiveConf();
    }
    RelOptSchema schema = HiveRelJsonSchemaReader.read(json, conf, cluster.getTypeFactory());
    RelJsonReader reader = new RelJsonReader(
        cluster,
        schema,
        null,
        t -> t.withOperatorTable(chain(new HiveSqlOperatorTable(), SqlStdOperatorTable.instance())));
    // At the moment we assume that the JSON plan always has a top-level field "CBOPlan"
    // that contains the actual plan that can be handled by RelJsonReader.
    return reader.read(new ObjectMapper().readTree(json).get("CBOPlan").toString());
  }
}
