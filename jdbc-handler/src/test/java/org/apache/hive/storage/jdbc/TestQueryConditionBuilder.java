/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.storage.jdbc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfig;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Scanner;

import static org.hamcrest.Matchers.equalToIgnoringWhiteSpace;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

public class TestQueryConditionBuilder {

  private static String condition1;
  private static String condition2;


  @BeforeClass
  public static void setup() throws IOException {
    condition1 = readFileContents("condition1.xml");
    condition2 = readFileContents("condition2.xml");
  }


  private static String readFileContents(String name) throws IOException {
    try (Scanner s = new Scanner(TestQueryConditionBuilder.class.getClassLoader().getResourceAsStream(name))) {
      return s.useDelimiter("\\Z").next();
    }
  }


  @Test
  public void testSimpleCondition_noTranslation() {
    Configuration conf = new Configuration();
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, condition1);
    conf.set(serdeConstants.LIST_COLUMNS, "visitor_id,sentiment,tracking_id");
    String condition = QueryConditionBuilder.getInstance().buildCondition(conf);

    assertThat(condition, is(notNullValue()));
    assertThat(condition, is(equalToIgnoringWhiteSpace("(visitor_id = 'x')")));
  }


  @Test
  public void testSimpleCondition_withTranslation() {
    Configuration conf = new Configuration();
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, condition1);
    conf.set(serdeConstants.LIST_COLUMNS, "visitor_id,sentiment,tracking_id");
    conf.set(JdbcStorageConfig.COLUMN_MAPPING.getPropertyName(),
        "visitor_id=vid, sentiment=sentiment, tracking_id=tracking_id");
    String condition = QueryConditionBuilder.getInstance().buildCondition(conf);

    assertThat(condition, is(notNullValue()));
    assertThat(condition, is(equalToIgnoringWhiteSpace("(vid = 'x')")));
  }


  @Test
  public void testSimpleCondition_withDateType() {
    Configuration conf = new Configuration();
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, condition1);
    conf.set(serdeConstants.LIST_COLUMNS, "visitor_id,sentiment,tracking_id");
    conf.set(JdbcStorageConfig.COLUMN_MAPPING.getPropertyName(),
        "visitor_id=vid:date, sentiment=sentiment, tracking_id=tracking_id");
    String condition = QueryConditionBuilder.getInstance().buildCondition(conf);

    assertThat(condition, is(notNullValue()));
    assertThat(condition, is(equalToIgnoringWhiteSpace("({d vid} = 'x')")));
  }


  @Test
  public void testSimpleCondition_withVariedCaseMappings() {
    Configuration conf = new Configuration();
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, condition1);
    conf.set(serdeConstants.LIST_COLUMNS, "visitor_ID,sentiment,tracking_id");
    conf.set(JdbcStorageConfig.COLUMN_MAPPING.getPropertyName(),
        "visitor_id=VID:date, sentiment=sentiment, tracking_id=tracking_id");
    String condition = QueryConditionBuilder.getInstance().buildCondition(conf);

    assertThat(condition, is(notNullValue()));
    assertThat(condition, is(equalToIgnoringWhiteSpace("({d vid} = 'x')")));
  }


  @Test
  public void testMultipleConditions_noTranslation() {
    Configuration conf = new Configuration();
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, condition2);
    conf.set(serdeConstants.LIST_COLUMNS, "visitor_id,sentiment,tracking_id");
    String condition = QueryConditionBuilder.getInstance().buildCondition(conf);

    assertThat(condition, is(notNullValue()));
    assertThat(condition, is(equalToIgnoringWhiteSpace("((visitor_id = 'x') and (sentiment = 'y'))")));
  }


  @Test
  public void testMultipleConditions_withTranslation() {
    Configuration conf = new Configuration();
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, condition2);
    conf.set(serdeConstants.LIST_COLUMNS, "visitor_id,sentiment,tracking_id");
    conf.set(JdbcStorageConfig.COLUMN_MAPPING.getPropertyName(), "visitor_id=v,sentiment=s,tracking_id=t");
    String condition = QueryConditionBuilder.getInstance().buildCondition(conf);

    assertThat(condition, is(notNullValue()));
    assertThat(condition, is(equalToIgnoringWhiteSpace("((v = 'x') and (s = 'y'))")));
  }


  @Test
  public void testWithNullConf() {
    String condition = QueryConditionBuilder.getInstance().buildCondition(null);
    assertThat(condition, is(notNullValue()));
    assertThat(condition.trim().isEmpty(), is(true));
  }


  @Test
  public void testWithUndefinedFilterExpr() {
    Configuration conf = new Configuration();
    conf.set(serdeConstants.LIST_COLUMNS, "visitor_id,sentiment,tracking_id");
    conf.set(JdbcStorageConfig.COLUMN_MAPPING.getPropertyName(), "visitor_id=v,sentiment=s,tracking_id=t");
    String condition = QueryConditionBuilder.getInstance().buildCondition(conf);

    assertThat(condition, is(notNullValue()));
    assertThat(condition.trim().isEmpty(), is(true));
  }

}
