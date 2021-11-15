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

package org.apache.hadoop.hive.metastore.tools.metatool;

import java.util.Collection;
import java.util.Iterator;

class MetaToolTaskExecuteJDOQLQuery extends MetaToolTask {
  @Override
  void execute() {
    String query = getCl().getJDOQLQuery();
      if (query.toLowerCase().trim().startsWith("select")) {
        try {
        executeJDOQLSelect(query);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      } else if (query.toLowerCase().trim().startsWith("update")) {
        try {
          executeJDOQLUpdate(query);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      } else {
        throw new IllegalArgumentException("HiveMetaTool:Unsupported statement type, only select and update supported");
      }
  }

  private void executeJDOQLSelect(String query) throws Exception {
    System.out.println("Executing query: " + query);
    Collection<?> result = getObjectStore().executeJDOQLSelect(query);
    if (result != null) {
      Iterator<?> iter = result.iterator();
      while (iter.hasNext()) {
        Object o = iter.next();
        System.out.println(o.toString());
      }
    } else {
      System.err.println("Encountered error during executeJDOQLSelect");
    }
  }

  private void executeJDOQLUpdate(String query) throws Exception {
    System.out.println("Executing query: " + query);
    long numUpdated = getObjectStore().executeJDOQLUpdate(query);
    if (numUpdated >= 0) {
      System.out.println("Number of records updated: " + numUpdated);
    } else {
      System.err.println("Encountered error during executeJDOQL - commit of JDO transaction failed.");
    }
  }
}
