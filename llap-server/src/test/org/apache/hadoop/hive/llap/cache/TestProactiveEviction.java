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
package org.apache.hadoop.hive.llap.cache;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.llap.ProactiveEviction;
import org.apache.hadoop.hive.llap.ProactiveEviction.Request;
import org.apache.hadoop.hive.llap.ProactiveEviction.Request.Builder;

import org.junit.Test;

import static org.apache.hadoop.hive.llap.cache.TestCacheContentsTracker.cacheTagBuilder;
import static org.junit.Assert.assertEquals;

/**
 * Test cases for proactive LLAP cache eviction.
 */
public class TestProactiveEviction {

  private static final CacheTag[] TEST_TAGS = new CacheTag[] {
    cacheTagBuilder("fx.rates", "from=USD", "to=HUF"),
    cacheTagBuilder("fx.rates", "from=USD", "to=EUR"),
    cacheTagBuilder("fx.rates", "from=USD", "to=EUR"),
    cacheTagBuilder("fx.rates", "from=USD", "to=EUR"),
    cacheTagBuilder("fx.rates", "from=EUR", "to=HUF"),
    cacheTagBuilder("fx.futures", "ccy=EUR"),
    cacheTagBuilder("fx.futures", "ccy=JPY"),
    cacheTagBuilder("fx.futures", "ccy=JPY"),
    cacheTagBuilder("fx.futures", "ccy=USD"),
    cacheTagBuilder("fx.centralbanks"),
    cacheTagBuilder("fx.centralbanks"),
    cacheTagBuilder("fx.centralbanks"),
    cacheTagBuilder("equity.prices", "ex=NYSE"),
    cacheTagBuilder("equity.prices", "ex=NYSE"),
    cacheTagBuilder("equity.prices", "ex=NASDAQ"),
    cacheTagBuilder("fixedincome.bonds"),
    cacheTagBuilder("fixedincome.bonds"),
    cacheTagBuilder("fixedincome.yieldcurves")
  };

  @Test
  public void testCachetagAndRequestMatching() throws Exception {
    assertMatchOnTags(Builder.create().addDb("fx"), "111111111111000000");
    assertMatchOnTags(Builder.create().addTable("fx", "futures"), "000001111000000000");
    assertMatchOnTags(Builder.create().addPartitionOfATable("fx", "futures", buildParts("ccy", "JPY")),
    "000000110000000000");
    assertMatchOnTags(Builder.create().addPartitionOfATable("equity", "prices", buildParts("ex", "NYSE"))
        .addPartitionOfATable("equity", "prices", buildParts("ex", "NYSE")),"000000000000110000");
    assertMatchOnTags(Builder.create().addTable("fx", "rates").addTable("fx", "futures"),
        "111111111000000000");
    assertMatchOnTags(Builder.create().addPartitionOfATable("fx", "rates", buildParts("from", "PLN")),
        "000000000000000000");
    assertMatchOnTags(Builder.create().addTable("fixedincome", "bonds"), "000000000000000110");
    assertMatchOnTags(Builder.create().addPartitionOfATable("fx", "rates", buildParts("from", "EUR", "to", "HUF")),
        "000010000000000000");
  }

  private static LinkedHashMap buildParts(String... vals) {
    LinkedHashMap<String, String> ret = new LinkedHashMap<>();
    for (int i = 0; i < vals.length; i+=2) {
      ret.put(vals[i], vals[i+1]);
    }
    return ret;
  }

  private static void assertMatchOnTags(Builder requestBuilder, String expected) {
    assert expected.length() == TEST_TAGS.length;
    // Marshal + unmarshal
    Request request = Builder.create().fromProtoRequest(requestBuilder.build().toProtoRequests().get(0)).build();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < TEST_TAGS.length; ++i) {
      sb.append(request.isTagMatch(TEST_TAGS[i]) ? '1' : '0');
    }
    assertEquals(expected, sb.toString());
  }

}
