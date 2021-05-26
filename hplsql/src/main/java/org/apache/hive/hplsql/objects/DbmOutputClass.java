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

package org.apache.hive.hplsql.objects;

import static org.apache.hive.hplsql.objects.MethodParams.Arity.UNARY;

public class DbmOutputClass implements HplClass {
  public static final DbmOutputClass INSTANCE = new DbmOutputClass();
  private final MethodDictionary<DbmOutput> methodDictionary = new MethodDictionary();

  private DbmOutputClass() {
    methodDictionary.put("put_line", (self, args) -> {
      UNARY.check("put_line", args);
      return self.putLine(args);
    });
  }

  @Override
  public DbmOutput newInstance() {
    return new DbmOutput(this);
  }

  @Override
  public MethodDictionary methodDictionary() {
    return methodDictionary;
  }
}
