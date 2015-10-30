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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;

public class CompositeHBaseKeyFactory<T extends HBaseCompositeKey> extends DefaultHBaseKeyFactory {

  public static final Logger LOG = LoggerFactory.getLogger(CompositeHBaseKeyFactory.class);

  private final Class<T> keyClass;
  private final Constructor constructor;

  private Configuration conf;

  public CompositeHBaseKeyFactory(Class<T> keyClass) throws Exception {
    // see javadoc of HBaseCompositeKey
    this.keyClass = keyClass;
    this.constructor = keyClass.getDeclaredConstructor(
        LazySimpleStructObjectInspector.class, Properties.class, Configuration.class);
  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) throws IOException {
    super.configureJobConf(tableDesc, jobConf);
    TableMapReduceUtil.addDependencyJars(jobConf, keyClass);
  }

  @Override
  public T createKey(ObjectInspector inspector) throws SerDeException {
    try {
      return (T) constructor.newInstance(inspector, properties, hbaseParams.getBaseConfiguration());
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }
}
