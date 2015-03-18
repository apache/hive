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

package org.apache.hadoop.hive.ql.exec;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ListSinkDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * For fetch task with operator tree, row read from FetchOperator is processed via operator tree
 * and finally arrives to this operator.
 */
public class ListSinkOperator extends Operator<ListSinkDesc> {

  public static final String OUTPUT_FORMATTER = "output.formatter";
  public static final String OUTPUT_PROTOCOL = "output.protocol";

  private transient List res;
  private transient FetchFormatter fetcher;
  private transient int numRows;

  @Override
  protected Collection<Future<?>> initializeOp(Configuration hconf) throws HiveException {
    Collection<Future<?>> result = super.initializeOp(hconf);
    try {
      fetcher = initializeFetcher(hconf);
    } catch (Exception e) {
      throw new HiveException(e);
    }
    return result;
  }

  private FetchFormatter initializeFetcher(Configuration conf) throws Exception {
    String formatterName = conf.get(OUTPUT_FORMATTER);
    FetchFormatter fetcher;
    if (formatterName != null && !formatterName.isEmpty()) {
      Class<? extends FetchFormatter> fetcherClass = Class.forName(formatterName, true,
          Utilities.getSessionSpecifiedClassLoader()).asSubclass(FetchFormatter.class);
      fetcher = ReflectionUtils.newInstance(fetcherClass, null);
    } else {
      fetcher = new DefaultFetchFormatter();
    }

    // selectively used by fetch formatter
    Properties props = new Properties();
    props.put(serdeConstants.SERIALIZATION_FORMAT, "" + Utilities.tabCode);
    props.put(serdeConstants.SERIALIZATION_NULL_FORMAT, getConf().getSerializationNullFormat());

    fetcher.initialize(conf, props);
    return fetcher;
  }

  public void reset(List res) {
    this.res = res;
    this.numRows = 0;
  }

  public int getNumRows() {
    return numRows;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void process(Object row, int tag) throws HiveException {
    try {
      res.add(fetcher.convert(row, inputObjInspectors[0]));
      numRows++;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  @Override
  public OperatorType getType() {
    return OperatorType.FORWARD;
  }
}
