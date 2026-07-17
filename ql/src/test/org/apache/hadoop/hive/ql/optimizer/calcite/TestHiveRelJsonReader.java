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

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.testutils.HiveTestEnvSetup;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.fail;

class TestHiveRelJsonReader {
  private static final Path TPCDS_RESULTS_PATH =
      Paths.get(HiveTestEnvSetup.HIVE_ROOT, "ql/src/test/results/clientpositive/perf/tpcds30tb/");

  static Stream<Path> inputJsonFiles() throws IOException {
    return Files.list(TPCDS_RESULTS_PATH.resolve("json"));
  }

  @ParameterizedTest
  @MethodSource("inputJsonFiles")
  void testReadJson(Path jsonFile) throws IOException {
    String jsonContent =
        Files.readAllLines(jsonFile).stream().filter(line -> !line.startsWith("Warning")).collect(Collectors.joining());
    // Use VolcanoPlanner to be able to set the ConventionTraitDef in the cluster,
    // otherwise all traitsets will have an empty set of traits which may lead to
    // assertions failures when creating Hive operators.
    VolcanoPlanner planner = new VolcanoPlanner(Contexts.of(new HiveConf()));
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    RelOptCluster cluster = RelOptCluster.create(planner, new HiveRexJsonBuilder());
    String actualPlan = RelOptUtil.toString(new HiveRelJsonReader(cluster).readJson(jsonContent));
    String expectedPlan = readExpectedPlan(jsonFile);
    assertLinesMatch(normalize(jsonFile, expectedPlan), normalize(jsonFile, actualPlan), "Failed for: " + jsonFile);
  }

  private static String readExpectedPlan(Path jsonFile) throws IOException {
    String cboFileName = jsonFile.getFileName().toString().replace("query", "cbo_query");
    Path cboFile = TPCDS_RESULTS_PATH.resolve("tez").resolve(cboFileName);
    if (!Files.exists(cboFile)) {
      fail("CBO file not found for JSON file: " + jsonFile + ", expected at: " + cboFile);
    }
    return new String(Files.readAllBytes(cboFile), Charset.defaultCharset());
  }

  private static Stream<String> normalize(Path file, String plan) {
    Function<String, String> normalizer = Function.identity();
    for (Normalizer i : Normalizer.values()) {
      if (i.affects(file.getFileName().toString())) {
        normalizer = normalizer.andThen(i::apply);
      }
    }
    return Arrays.stream(plan.split("\\R"))
        .filter(line -> !line.startsWith("Warning"))
        .filter(line -> !line.startsWith("CBO PLAN:"))
        .filter(line -> !line.trim().isEmpty())
        .map(normalizer);
  }

  /**
   * Normalization logic for addressing small discrepancies between plans.
   */
  private enum Normalizer {
    /**
     * A normalizer for scientific notation discrepancies.
     * <p>
     * Normalizes the input line by converting occurrences of scientific notation from numbers.
     */
    SCIENTIFIC_NOTATION(
        Pattern.compile("\\d+(\\.\\d+)?E\\d+"),
        num -> new BigDecimal(num).toPlainString(),
        "query34.q.out",
        "query39.q.out",
        "query73.q.out",
        "query83.q.out"),
    /**
     * A normalizer for COUNT function discrepancies. At the moment the deserializer fails to
     * distinguish between COUNT (from {@link org.apache.calcite.sql.fun.SqlStdOperatorTable}) and
     * count (from {@link org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlCountAggFunction})
     * operators. The deserializer always picks the latter no matter which one is really present
     * in the original plan.
     * <p>
     * Normalizes the input line by converting occurrences of the COUNT aggregate function name to lowercase implicitly
     * converting the SQL standard operator to Hive built-in.
     */
    COUNT(
        Pattern.compile("COUNT\\(", Pattern.CASE_INSENSITIVE),
        String::toLowerCase,
        "query6.q.out",
        "query14.q.out",
        "query44.q.out",
        "query54.q.out",
        "query58.q.out");

    private final Pattern pattern;
    private final UnaryOperator<String> replacer;
    private final Set<String> affectedFiles;

    Normalizer(Pattern pattern, UnaryOperator<String> replacer, String... files) {
      this.pattern = pattern;
      this.replacer = replacer;
      this.affectedFiles = ImmutableSet.copyOf(files);
    }

    boolean affects(String fileName) {
      return affectedFiles.contains(fileName);
    }

    String apply(String line) {
      Matcher matcher = pattern.matcher(line);
      StringBuilder sb = new StringBuilder();
      while (matcher.find()) {
        matcher.appendReplacement(sb, replacer.apply(matcher.group()));
      }
      matcher.appendTail(sb);
      return sb.toString();
    }

  }
}
