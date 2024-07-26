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
        "CREATEtABLE_AS_sELECT," + HiveOperation.QUERY.name());

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

    assertThat(exception.getMessage(), is("org.apache.hadoop.hive.ql.plan.HiveOperation.Invalid"));
  }

  @Test
  public void testCreateFilterPredicateFromConfThrowsExceptionWhenNoneAndAnyOtherConstantPresent() {
    HiveConf conf = new HiveConf();
    conf.set(HiveConf.ConfVars.HIVE_LINEAGE_STATEMENT_FILTER.varname, "None," + HiveOperation.QUERY.name());

    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> Generator.createFilterPredicateFromConf(conf)
    );

    assertThat(exception.getMessage(), is("No other value can be specified when None is present!"));
  }
}