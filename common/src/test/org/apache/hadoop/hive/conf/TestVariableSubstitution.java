/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.conf;

import junit.framework.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestVariableSubstitution {
  private static class LocalMySource {
    final Map<String, String> map = new HashMap<>();

    public void put(String k, String v) {
      map.put(k, v);
    }

    public String get(String k) {
      return map.get(k);
    }
  }

  private static LocalMySource getMySource() {
    return localSource.get();
  }

  private static ThreadLocal<LocalMySource> localSource = new ThreadLocal<LocalMySource>() {
    @Override protected LocalMySource initialValue() {
      return new LocalMySource();
    }
  };

  @Test public void testVariableSource() throws InterruptedException {
    final VariableSubstitution variableSubstitution =
        new VariableSubstitution(new HiveVariableSource() {
          @Override public Map<String, String> getHiveVariable() {
            return TestVariableSubstitution.getMySource().map;
          }
        });

    String v = variableSubstitution.substitute(new HiveConf(), "${a}");
    Assert.assertEquals("${a}", v);
    TestVariableSubstitution.getMySource().put("a", "b");
    v = variableSubstitution.substitute(new HiveConf(), "${a}");
    Assert.assertEquals("b", v);
  }
}
