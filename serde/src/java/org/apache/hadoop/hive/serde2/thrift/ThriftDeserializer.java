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

package org.apache.hadoop.hive.serde2.thrift;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractDeserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.thrift.protocol.TProtocolFactory;

/**
 * ThriftDeserializer.
 *
 */
public class ThriftDeserializer extends AbstractDeserializer {

  private ThriftByteStreamTypedSerDe tsd;

  public ThriftDeserializer() {
  }

  @Override
  public void initialize(Configuration job, Properties tbl)
      throws SerDeException {
    try {
      // both the classname and the protocol name are Table properties
      // the only hardwired assumption is that records are fixed on a
      // per Table basis

      String className = tbl
          .getProperty(org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_CLASS);
      Class<?> recordClass = job.getClassByName(className);

      String protoName = tbl
          .getProperty(org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT);
      if (protoName == null) {
        protoName = "TBinaryProtocol";
      }
      // For backward compatibility
      protoName = protoName.replace("com.facebook.thrift.protocol",
          "org.apache.thrift.protocol");

      TProtocolFactory tp = TReflectionUtils
          .getProtocolFactoryByName(protoName);
      tsd = new ThriftByteStreamTypedSerDe(recordClass, tp, tp);

    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  @Override
  public Object deserialize(Writable field) throws SerDeException {
    return tsd.deserialize(field);
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return tsd.getObjectInspector();
  }

  @Override
  public SerDeStats getSerDeStats() {
    // no support for statistics
    return null;
  }
}
