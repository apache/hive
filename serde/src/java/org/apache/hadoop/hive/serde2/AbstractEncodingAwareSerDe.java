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

package org.apache.hadoop.hive.serde2;

import java.nio.charset.Charset;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

/**
 * AbstractEncodingAwareSerDe aware the encoding from table properties,
 * transform data from specified charset to UTF-8 during serialize, and
 * transform data from UTF-8 to specified charset during deserialize.
 */
public abstract class AbstractEncodingAwareSerDe extends AbstractSerDe {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractEncodingAwareSerDe.class);
  protected Charset charset;

  @Override
  @Deprecated
  public void initialize(Configuration conf, Properties tbl)
      throws SerDeException {
    charset = Charset.forName(tbl.getProperty(serdeConstants.SERIALIZATION_ENCODING, "UTF-8"));
    if (this.charset.equals(Charsets.ISO_8859_1) || this.charset.equals(Charsets.US_ASCII)) {
      LOG.warn("The data may not be properly converted to target charset " + charset);
    }
  }

  @Override
  public final Writable serialize(Object obj, ObjectInspector objInspector)
      throws SerDeException {
    Writable result = doSerialize(obj, objInspector);
    if (!this.charset.equals(Charsets.UTF_8)) {
      result = transformFromUTF8(result);
    }
    return result;
  }

  /**
   * transform Writable data from UTF-8 to charset before serialize.
   * @param blob
   * @return
   */
  protected abstract Writable transformFromUTF8(Writable blob);

  protected abstract Writable doSerialize(Object obj, ObjectInspector objInspector) throws SerDeException;

  @Override
  public final Object deserialize(Writable blob) throws SerDeException {
    if (!this.charset.equals(Charsets.UTF_8)) {
      blob = transformToUTF8(blob);
    }
    return doDeserialize(blob);
  }

  /**
   * transform Writable data from charset to UTF-8 before doDeserialize.
   * @param blob
   * @return
   */
  protected abstract Writable transformToUTF8(Writable blob);

  protected abstract Object doDeserialize(Writable blob) throws SerDeException;
}
