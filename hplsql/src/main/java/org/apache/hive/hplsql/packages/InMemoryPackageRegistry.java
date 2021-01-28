/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hive.hplsql.packages;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class InMemoryPackageRegistry implements PackageRegistry {
  private Map<String, Source> registry = new HashMap<>();

  @Override
  public Optional<String> findPackage(String name) {
    Source src = registry.get(name.toUpperCase());
    return src == null
            ? Optional.empty()
            : Optional.of(src.header + ";\n" + src.body);
  }

  @Override
  public void createPackage(String name, String header) {
    registry.put(name, new Source(header, ""));
  }

  @Override
  public void createPackageBody(String name, String body) {
    registry.getOrDefault(name, new Source("", "")).body = body;
  }

  @Override
  public void dropPackage(String name) {
    registry.remove(name);
  }

  private static class Source {
    String header;
    String body;

    public Source(String header, String body) {
      this.header = header;
      this.body = body;
    }
  }
}
