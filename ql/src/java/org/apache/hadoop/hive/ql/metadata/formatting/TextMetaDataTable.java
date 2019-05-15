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

package org.apache.hadoop.hive.ql.metadata.formatting;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;

public class TextMetaDataTable {

  List<List<String>> table = new ArrayList<>();

  public void addRow(String... values) {
    table.add(Lists.<String> newArrayList(values));
  }

  public String renderTable(boolean isOutputPadded) {
    StringBuilder str = new StringBuilder();
    for (List<String> row : table) {
      MetaDataFormatUtils.formatOutput(row.toArray(new String[] {}), str, isOutputPadded, isOutputPadded);
    }
    return str.toString();
  }

  public void transpose() {
    if (table.size() == 0) {
      return;
    }
    List<List<String>> newTable = new ArrayList<List<String>>();
    for (int i = 0; i < table.get(0).size(); i++) {
      newTable.add(new ArrayList<>());
    }
    for (List<String> srcRow : table) {
      if (newTable.size() != srcRow.size()) {
        throw new RuntimeException("invalid table size");
      }
      for (int i = 0; i < srcRow.size(); i++) {
        newTable.get(i).add(srcRow.get(i));
      }
    }
    table = newTable;
  }

}
