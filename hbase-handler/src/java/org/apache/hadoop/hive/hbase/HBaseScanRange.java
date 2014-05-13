/**
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

package org.apache.hadoop.hive.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.io.BytesWritable;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class HBaseScanRange implements Serializable {

  private byte[] startRow;
  private byte[] stopRow;

  private List<FilterDesc> filterDescs = new ArrayList<FilterDesc>();

  public byte[] getStartRow() {
    return startRow;
  }

  public void setStartRow(byte[] startRow) {
    this.startRow = startRow;
  }

  public byte[] getStopRow() {
    return stopRow;
  }

  public void setStopRow(byte[] stopRow) {
    this.stopRow = stopRow;
  }

  public void addFilter(Filter filter) throws Exception {
    Class<? extends Filter> clazz = filter.getClass();
    clazz.getMethod("parseFrom", byte[].class);   // valiade
    filterDescs.add(new FilterDesc(clazz.getName(), filter.toByteArray()));
  }

  public void setup(Scan scan, Configuration conf) throws Exception {
    if (startRow != null) {
      scan.setStartRow(startRow);
    }
    if (stopRow != null) {
      scan.setStopRow(stopRow);
    }
    if (filterDescs.isEmpty()) {
      return;
    }
    if (filterDescs.size() == 1) {
      scan.setFilter(filterDescs.get(0).toFilter(conf));
      return;
    }
    List<Filter> filters = new ArrayList<Filter>();
    for (FilterDesc filter : filterDescs) {
      filters.add(filter.toFilter(conf));
    }
    scan.setFilter(new FilterList(filters));
  }

  public String toString() {
    return (startRow == null ? "" : new BytesWritable(startRow).toString()) + " ~ " +
        (stopRow == null ? "" : new BytesWritable(stopRow).toString());
  }

  private static class FilterDesc implements Serializable {

    private String className;
    private byte[] binary;

    public FilterDesc(String className, byte[] binary) {
      this.className = className;
      this.binary = binary;
    }

    public Filter toFilter(Configuration conf) throws Exception {
      return (Filter) getFactoryMethod(className, conf).invoke(null, binary);
    }

    private Method getFactoryMethod(String className, Configuration conf) throws Exception {
      Class<?> clazz = conf.getClassByName(className);
      return clazz.getMethod("parseFrom", byte[].class);
    }
  }
}
