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

package org.apache.hadoop.hive.ql.optimizer.lineage;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.junit.Test;

import java.util.function.Predicate;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

public class TestGenerator {
  @Test
  public void testCreateFilterPredicateFromConf() {
    HiveConf conf = new HiveConf();
    conf.set(HiveConf.ConfVars.HIVE_LINEAGE_STATEMENT_FILTER.varname,
        "CREATE_tABLE_AS_sELECT," + HiveOperation.QUERY.name());

    Predicate<ParseContext> predicate = Generator.createFilterPredicateFromConf(conf);

    ParseContext parseContext = new ParseContext();
    QueryProperties queryProperties = new QueryProperties();
    queryProperties.setCTAS(true);
    parseContext.setQueryProperties(queryProperties);
    assertThat(predicate.test(parseContext), is(true));

    parseContext = new ParseContext();
    queryProperties = new QueryProperties();
    queryProperties.setQuery(true);
    parseContext.setQueryProperties(queryProperties);
    assertThat(predicate.test(parseContext), is(true));

    parseContext = new ParseContext();
    queryProperties = new QueryProperties();
    queryProperties.setView(true);
    parseContext.setQueryProperties(queryProperties);
    assertThat(predicate.test(parseContext), is(false));
  }

  @Test
  public void testCreateFilterPredicateFromConfReturnsAlwaysTrueWhenSettingIsNotPresent() {
    HiveConf conf = new HiveConf();

    Predicate<ParseContext> predicate = Generator.createFilterPredicateFromConf(conf);

    assertThat(predicate.test(new ParseContext()), is(true));
  }

  @Test
  public void testCreateFilterPredicateFromConfReturnsAlwaysFalseWhenSettingValueIsEmptyString() {
    HiveConf conf = new HiveConf();
    conf.set(HiveConf.ConfVars.HIVE_LINEAGE_STATEMENT_FILTER.varname, "");

    Predicate<ParseContext> predicate = Generator.createFilterPredicateFromConf(conf);

    assertThat(predicate.test(new ParseContext()), is(false));
  }

  @Test
  public void testCreateFilterPredicateFromConfThrowsExceptionWhenInputStringIsInvalid() {
    HiveConf conf = new HiveConf();
    conf.set(HiveConf.ConfVars.HIVE_LINEAGE_STATEMENT_FILTER.varname, "Invalid");

    EnumConstantNotPresentException exception = assertThrows(
        EnumConstantNotPresentException.class,
        () -> Generator.createFilterPredicateFromConf(conf)
    );

    assertThat(exception.getMessage(), is(
        "org.apache.hadoop.hive.ql.optimizer.lineage.Generator$LineageInfoFilter.Invalid"));
  }

  @Test
  public void testCreateFilterPredicateFromConfThrowsExceptionWhenNoneAndAnyOtherConstantPresent() {
    HiveConf conf = new HiveConf();
    conf.set(HiveConf.ConfVars.HIVE_LINEAGE_STATEMENT_FILTER.varname, "None," + HiveOperation.QUERY.name());

    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> Generator.createFilterPredicateFromConf(conf)
    );

    assertThat(exception.getMessage(), is("No other value can be specified when NONE is present!"));
  }
}