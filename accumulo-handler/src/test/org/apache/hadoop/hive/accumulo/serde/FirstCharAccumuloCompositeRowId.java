/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gets the first character of each string in a struct
 */
public class FirstCharAccumuloCompositeRowId extends AccumuloCompositeRowId {
  private static final Logger log = LoggerFactory.getLogger(FirstCharAccumuloCompositeRowId.class);

  private Properties tbl;
  private Configuration conf;

  public FirstCharAccumuloCompositeRowId(LazySimpleStructObjectInspector oi, Properties tbl,
      Configuration conf) {
    super(oi);
    this.tbl = tbl;
    this.conf = conf;
  }

  @Override
  public Object getField(int fieldID) {
    String bytesAsString = new String(bytes.getData(), start, length);

    log.info("Data: " + bytesAsString + ", " + Arrays.toString(bytes.getData()));

    // The separator for the hive row would be using \x02, so the separator for this struct would be
    // \x02 + 1 = \x03
    char separator = (char) ((int) oi.getSeparator() + 1);

    log.info("Separator: " + String.format("%04x", (int) separator));

    // Get the character/byte at the offset in the string equal to the fieldID
    String[] fieldBytes = StringUtils.split(bytesAsString, separator);

    log.info("Fields: " + Arrays.toString(fieldBytes));

    return toLazyObject(fieldID, new byte[] {(byte) fieldBytes[fieldID].charAt(0)});
  }
}
