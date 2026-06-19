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

package org.apache.hadoop.hive.accumulo.serde;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.accumulo.AccumuloDefaultIndexScanner;
import org.apache.hadoop.hive.accumulo.AccumuloIndexScanner;
import org.apache.hadoop.hive.accumulo.AccumuloIndexScannerException;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;


/**
 * Accumulo Index Parameters for Hive tables.
 */
public class AccumuloIndexParameters {
  public static final int DEFAULT_MAX_ROWIDS = 20000;
  public static final String INDEX_SCANNER = "accumulo.index.scanner";
  public static final String MAX_INDEX_ROWS = "accumulo.index.rows.max";
  public static final String INDEXED_COLUMNS = "accumulo.indexed.columns";
  public static final String INDEXTABLE_NAME = "accumulo.indextable.name";
  private static final Set<String> EMPTY_SET = new HashSet<String>();
  private Configuration conf;

  public AccumuloIndexParameters(Configuration conf) {
    this.conf = conf;
  }

  public String getIndexTable() {
    return this.conf.get(INDEXTABLE_NAME);
  }

  public int getMaxIndexRows() {
    return this.conf.getInt(MAX_INDEX_ROWS, DEFAULT_MAX_ROWIDS);
  }

  public final Set<String> getIndexColumns() {
    String colmap = conf.get(INDEXED_COLUMNS);
    if (colmap != null) {
      Set<String> cols = new HashSet<String>();
        for (String col : colmap.split(",")) {
          cols.add(col.trim());
        }
        return cols;
    }
    return EMPTY_SET;
  }


  public final Authorizations getTableAuths() {
    String auths = conf.get(AccumuloSerDeParameters.AUTHORIZATIONS_KEY);
    if (auths != null && !auths.isEmpty()) {
      return new Authorizations(auths.trim().getBytes(StandardCharsets.UTF_8));
    }
    return new Authorizations();
  }

  public Configuration getConf() {
    return conf;
  }

  public final AccumuloIndexScanner createScanner() throws AccumuloIndexScannerException {
    AccumuloIndexScanner handler;

    String classname = conf.get(INDEX_SCANNER);
    if (classname != null) {
      try {
        handler = (AccumuloIndexScanner) Class.forName(classname).newInstance();
      } catch (ClassCastException | InstantiationException |  IllegalAccessException
          | ClassNotFoundException e) {
        throw new AccumuloIndexScannerException("Cannot use index scanner class: " + classname, e);
      }
    } else {
      handler = new AccumuloDefaultIndexScanner();
    }
    if (handler != null) {
      handler.init(conf);
    }
    return handler;
  }
}
