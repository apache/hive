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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.InstantFormatter;
import org.apache.hadoop.io.Text;

import java.nio.charset.CharacterCodingException;
import java.text.ParseException;

/**
 * Return Unix Timestamp.
 * Extends {@link VectorUDFTimestampFieldString}
 */
public final class VectorUDFUnixTimeStampString extends VectorUDFTimestampFieldString {

  private static final long serialVersionUID = 1L;

  private transient InstantFormatter formatter;

  public VectorUDFUnixTimeStampString(int colNum, int outputColumnNum) {
    super(colNum, outputColumnNum, -1, -1);
  }

  public VectorUDFUnixTimeStampString() {
    super();
  }

  @Override
  public void transientInit(Configuration conf) throws HiveException {
    super.transientInit(conf);
    if (formatter == null) {
      formatter = InstantFormatter.ofConfiguration(conf);
    }
  }

  @Override
  protected long getField(byte[] bytes, int start, int length) throws ParseException {
    try {
      return formatter.parse(Text.decode(bytes, start, length)).getEpochSecond();
    } catch (CharacterCodingException | RuntimeException e) {
      throw new ParseException(e.getMessage(), 0);
    }
  }
}
