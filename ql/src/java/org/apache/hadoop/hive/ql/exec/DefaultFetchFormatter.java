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

import java.io.IOException;
import java.util.Properties;

import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_NULL_FORMAT;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.DelimitedJSONSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * serialize row by user specified serde and call toString() to make string type result
 */
public class DefaultFetchFormatter<T> implements FetchFormatter<String> {

  private SerDe mSerde;

  @Override
  public void initialize(Configuration hconf, Properties props) throws HiveException {
    try {
      mSerde = initializeSerde(hconf, props);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  private SerDe initializeSerde(Configuration conf, Properties props) throws Exception {
    String serdeName = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEFETCHOUTPUTSERDE);
    Class<? extends SerDe> serdeClass = Class.forName(serdeName, true,
        JavaUtils.getClassLoader()).asSubclass(SerDe.class);
    // cast only needed for Hadoop 0.17 compatibility
    SerDe serde = ReflectionUtils.newInstance(serdeClass, null);

    Properties serdeProps = new Properties();
    if (serde instanceof DelimitedJSONSerDe) {
      serdeProps.put(SERIALIZATION_FORMAT, props.getProperty(SERIALIZATION_FORMAT));
      serdeProps.put(SERIALIZATION_NULL_FORMAT, props.getProperty(SERIALIZATION_NULL_FORMAT));
    }
    serde.initialize(conf, serdeProps);
    return serde;
  }

  @Override
  public String convert(Object row, ObjectInspector rowOI) throws Exception {
    return mSerde.serialize(row, rowOI).toString();
  }

  @Override
  public void close() throws IOException {
  }
}
