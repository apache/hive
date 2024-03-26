/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite;

import org.apache.calcite.adapter.tpcds.TpcdsSchema;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.testutils.HiveTestEnvSetup;

import org.approvaltests.Approvals;
import org.approvaltests.core.Options;
import org.approvaltests.reporters.AutoApproveWhenEmptyReporter;
import org.approvaltests.reporters.Junit4Reporter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class TestCommonTableExpressionIdentitySuggester {
  private static final Pattern SET_PATTERN = Pattern.compile("set.+;", Pattern.CASE_INSENSITIVE);
  private static final Pattern EXPLAIN_CBO_PATTERN = Pattern.compile("explain\\s+cbo", Pattern.CASE_INSENSITIVE);
  private static final Pattern DAYS_PATTERN = Pattern.compile("(\\d+) days");
  private final int queryId;

  public TestCommonTableExpressionIdentitySuggester(int queryId) {
    this.queryId = queryId;
  }

  @Parameterized.Parameters(name = "query={0}")
  public static List<Integer> tpcdsQueries() {
    return Arrays.asList(1, 2, 9, 14, 23, 24, 31, 33, 39, 44, 45, 54, 56, 58, 59, 60, 61, 64, 65, 75, 81, 83, 88, 95);
  }

  @Test
  public void testSuggest() throws IOException, SqlParseException, URISyntaxException {
    final String queryName = "cbo_query" + queryId;
    Path queryPath =
        Paths.get(HiveTestEnvSetup.HIVE_ROOT, "ql/src/test/queries/clientpositive/perf/" + queryName + ".q");
    String q = new String(Files.readAllBytes(queryPath));
    q = SET_PATTERN.matcher(q).replaceAll("");
    q = EXPLAIN_CBO_PATTERN.matcher(q).replaceAll("");
    q = DAYS_PATTERN.matcher(q).replaceAll("interval '$1' day");
    q = q.replaceAll("\\breturns\\b", "ret");
    q = q.replaceAll("\\bsubstr\\b", "substring");
    q = q.replaceAll("\\byear\\b", "`year`");
    q = q.replaceAll("d_date\\s+\\+\\s+(\\d+)", "d_date + interval '$1' day");
    q = q.replace(';', '\n');
    System.out.println(q);
    RelOptTable.ViewExpander viewExpander = (rowType, queryString, schemaPath, viewPath) -> null;
    CalciteSchema root = CalciteSchema.createRootSchema(false);
    root.add("tpcds", new TpcdsSchema(1));
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(new HiveTypeSystemImpl());
    Prepare.CatalogReader catalogReader =
        new CalciteCatalogReader(root, Collections.singletonList("tpcds"), typeFactory,
            CalciteConnectionConfig.DEFAULT);
    SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), catalogReader, typeFactory,
        SqlValidator.Config.DEFAULT.withSqlConformance(SqlConformanceEnum.BABEL));
    HepPlanner planner = new HepPlanner(HepProgram.builder().build());
    RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
    SqlToRelConverter s2r =
        new SqlToRelConverter(viewExpander, validator, catalogReader, cluster, StandardConvertletTable.INSTANCE,
            SqlToRelConverter.Config.DEFAULT);
    SqlParser parser = SqlParser.create(q,
        SqlParser.configBuilder().setConformance(SqlConformanceEnum.BABEL).setQuoting(Quoting.BACK_TICK).build());
    Options options = new Options().withReporter(new AutoApproveWhenEmptyReporter(new Junit4Reporter())).forFile()
        .withAdditionalInformation(queryName);
    SqlNode validNode = validator.validate(parser.parseQuery());
    RelNode queryPlan = s2r.convertQuery(validNode, false, true).rel;
    CommonTableExpressionIdentitySuggester suggester = new CommonTableExpressionIdentitySuggester();
    List<String> suggestions =
        suggester.suggest(queryPlan, new HiveConf()).stream().map(RelOptUtil::toString).collect(Collectors.toList());
    Approvals.verifyAll("", suggestions, options);
  }
}
