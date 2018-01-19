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

package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class TestUDFJson {

  @Test
  public void testJson() throws HiveException {

    String book0 = "{\"author\":\"Nigel Rees\",\"title\":\"Sayings of the Century\""
        + ",\"category\":\"reference\",\"price\":8.95}";
    String backet0 = "[1,2,{\"b\":\"y\",\"a\":\"x\"}]";
    String backet = "[" + backet0 + ",[3,4],[5,6]]";
    String backetFlat = backet0.substring(0, backet0.length() - 1) + ",3,4,5,6]";

    String book = "[" + book0 + ",{\"author\":\"Herman Melville\",\"title\":\"Moby Dick\","
        + "\"category\":\"fiction\",\"price\":8.99"
        + ",\"isbn\":\"0-553-21311-3\"},{\"author\":\"J. R. R. Tolkien\""
        + ",\"title\":\"The Lord of the Rings\",\"category\":\"fiction\""
        + ",\"reader\":[{\"age\":25,\"name\":\"bob\"},{\"age\":26,\"name\":\"jack\"}]"
        + ",\"price\":22.99,\"isbn\":\"0-395-19395-8\"}]";

    String json = "{\"store\":{\"fruit\":[{\"weight\":8,\"type\":\"apple\"},"
        + "{\"weight\":9,\"type\":\"pear\"}],\"basket\":" + backet + ",\"book\":" + book
        + ",\"bicycle\":{\"price\":19.95,\"color\":\"red\"}}"
        + ",\"email\":\"amy@only_for_json_udf_test.net\""
        + ",\"owner\":\"amy\",\"zip code\":\"94025\",\"fb:testid\":\"1234\"}";

    UDFJson udf = new UDFJson();

    runTest(json, "$.owner", "amy", udf);
    runTest(json, "$.store.bicycle", "{\"price\":19.95,\"color\":\"red\"}", udf);
    runTest(json, "$.store.book", book, udf);
    runTest(json, "$.store.book[0]", book0, udf);
    runTest(json, "$.store.book[*]", book, udf);
    runTest(json, "$.store.book[0].category", "reference", udf);
    runTest(json, "$.store.book[*].category", "[\"reference\",\"fiction\",\"fiction\"]", udf);
    runTest(json, "$.store.book[*].reader[0].age", "25", udf);
    runTest(json, "$.store.book[*].reader[*].age", "[25,26]", udf);
    runTest(json, "$.store.basket[0][1]", "2", udf);
    runTest(json, "$.store.basket[*]", backet, udf);
    runTest(json, "$.store.basket[*][0]", "[1,3,5]", udf);
    runTest(json, "$.store.basket[0][*]", backet0, udf);
    runTest(json, "$.store.basket[*][*]", backetFlat, udf);
    runTest(json, "$.store.basket[0][2].b", "y", udf);
    runTest(json, "$.store.basket[0][*].b", "[\"y\"]", udf);
    runTest(json, "$.non_exist_key", null, udf);
    runTest(json, "$.store.book[10]", null, udf);
    runTest(json, "$.store.book[0].non_exist_key", null, udf);
    runTest(json, "$.store.basket[*].non_exist_key", null, udf);
    runTest(json, "$.store.basket[0][*].non_exist_key", null, udf);
    runTest(json, "$.store.basket[*][*].non_exist_key", null, udf);
    runTest(json, "$.zip code", "94025", udf);
    runTest(json, "$.fb:testid", "1234", udf);
    runTest("{\"a\":\"b\nc\"}", "$.a", "b\nc", udf);
  }

  @Test
  public void testRootArray() throws HiveException {
    UDFJson udf = new UDFJson();

    runTest("[1,2,3]", "$[0]", "1", udf);
    runTest("[1,2,3]", "$.[0]", "1", udf);
    runTest("[1,2,3]", "$.[1]", "2", udf);
    runTest("[1,2,3]", "$[1]", "2", udf);

    runTest("[1,2,3]", "$[3]", null, udf);
    runTest("[1,2,3]", "$.[*]", "[1,2,3]", udf);
    runTest("[1,2,3]", "$[*]", "[1,2,3]", udf);
    runTest("[1,2,3]", "$", "[1,2,3]", udf);
    runTest("[{\"k1\":\"v1\"},{\"k2\":\"v2\"},{\"k3\":\"v3\"}]", "$[2]", "{\"k3\":\"v3\"}", udf);
    runTest("[{\"k1\":\"v1\"},{\"k2\":\"v2\"},{\"k3\":\"v3\"}]", "$[2].k3", "v3", udf);
    runTest("[{\"k1\":[{\"k11\":[1,2,3]}]}]", "$[0].k1[0].k11[1]", "2", udf);
    runTest("[{\"k1\":[{\"k11\":[1,2,3]}]}]", "$[0].k1[0].k11", "[1,2,3]", udf);
    runTest("[{\"k1\":[{\"k11\":[1,2,3]}]}]", "$[0].k1[0]", "{\"k11\":[1,2,3]}", udf);
    runTest("[{\"k1\":[{\"k11\":[1,2,3]}]}]", "$[0].k1", "[{\"k11\":[1,2,3]}]", udf);
    runTest("[{\"k1\":[{\"k11\":[1,2,3]}]}]", "$[0]", "{\"k1\":[{\"k11\":[1,2,3]}]}", udf);
    runTest("[[1,2,3],[4,5,6],[7,8,9]]", "$[1]", "[4,5,6]", udf);
    runTest("[[1,2,3],[4,5,6],[7,8,9]]", "$[1][0]", "4", udf);
    runTest("[\"a\",\"b\"]", "$[1]", "b", udf);
    runTest("[[\"a\",\"b\"]]", "$[0][1]", "b", udf);

    runTest("[1,2,3]", "[0]", null, udf);
    runTest("[1,2,3]", "$0", null, udf);
    runTest("[1,2,3]", "0", null, udf);
    runTest("[1,2,3]", "$.", null, udf);

    runTest("[1,2,3]", "$", "[1,2,3]", udf);
    runTest("{\"a\":4}", "$", "{\"a\":4}", udf);
  }

  protected void runTest(String json, String path, String exp, UDFJson udf) {
    Text res = udf.evaluate(json, path);
    if (exp == null) {
      Assert.assertNull(res);
    } else {
      Assert.assertNotNull(res);
      Assert.assertEquals("get_json_object test", exp, res.toString());
    }
  }
}
