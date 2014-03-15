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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hcatalog.data.transfer;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * This contains information obtained at master node to help prepare slave nodes
 * for writer. This class implements {@link Externalizable} so it can be
 * serialized using standard java mechanisms. Master should serialize it and
 * make it available to slaves to prepare for writes.
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.data.transfer.WriterContext} instead
 */
public class WriterContext implements Externalizable, Configurable {

  private static final long serialVersionUID = -5899374262971611840L;
  private Configuration conf;

  public WriterContext() {
    conf = new Configuration();
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(final Configuration config) {
    this.conf = config;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    conf.write(out);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException,
    ClassNotFoundException {
    conf.readFields(in);
  }
}
