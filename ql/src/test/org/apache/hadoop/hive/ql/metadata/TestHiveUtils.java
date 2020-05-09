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
package org.apache.hadoop.hive.ql.metadata;

import static org.apache.hadoop.hive.ql.metadata.HiveUtils.unparseIdentifier;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.Quotation;
import org.junit.Test;

/**
 * Test class for testing methods in {@link HiveUtils}.
 */
public class TestHiveUtils {
  @Test
  public void testUnparseIdentifierWithBackTicksWhenQuotationIsNone() {
    HiveConf conf = createConf(Quotation.NONE);
    String id = "any``id";

    String unparsed = unparseIdentifier(id, conf);

    assertThat(unparsed, is("`any``id`"));
  }

  @Test
  public void testUnparseIdentifierWithBackTicksWhenQuotationIsBackTicks() {
    HiveConf conf = createConf(Quotation.BACKTICKS);
    String id = "any``id";

    String unparsed = unparseIdentifier(id, conf);

    assertThat(unparsed, is("`any````id`"));
  }

  @Test
  public void testUnparseIdentifierWithBackTicksWhenQuotationIsStandard() {
    HiveConf conf = createConf(Quotation.STANDARD);
    String id = "any``id";

    String unparsed = unparseIdentifier(id, conf);

    assertThat(unparsed, is("`any````id`"));
  }

  @Test
  public void testUnparseIdentifierWithDoubleQuotesWhenQuotationIsNone() {
    HiveConf conf = createConf(Quotation.NONE);
    String id = "any\"\"id";

    String unparsed = unparseIdentifier(id, conf);

    assertThat(unparsed, is("`any\"\"id`"));
  }

  @Test
  public void testUnparseIdentifierWithDoubleQuotesWhenQuotationIsBackTicks() {
    HiveConf conf = createConf(Quotation.BACKTICKS);
    String id = "any\"\"id";

    String unparsed = unparseIdentifier(id, conf);

    assertThat(unparsed, is("`any\"\"id`"));
  }

  @Test
  public void testUnparseIdentifierWithDoubleQuotesWhenQuotationIsStandard() {
    HiveConf conf = createConf(Quotation.STANDARD);
    String id = "any\"\"id";

    String unparsed = unparseIdentifier(id, conf);

    assertThat(unparsed, is("`any\"\"id`"));
  }

  private HiveConf createConf(Quotation quotation) {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_QUOTEDID_SUPPORT, quotation.stringValue());
    return conf;
  }
}
