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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hive.common.util.ReflectionUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class InputFormatCache {

  /**
   * Interface for InputFormat classes that want to introduce a special logic while retrieved from this cache.
   * They can even return a new instance, apparently bypassing the cache, but still having the advantages of preventing
   * to call the original, expensive reflection call for instantiation.
   */
  public interface CacheableInputFormat<K, V> extends InputFormat<K, V> {
    default InputFormat<K, V> newInstance(InputFormat<K, V> cached){
      return cached;
    }
  }

  /**
   * A cache of InputFormat instances.
   */
  private static final Map<String, InputFormat<?, ?>> inputFormats = new HashMap<>();

  public static InputFormat getInputFormat(
      Class<? extends InputFormat> inputFormatClass, Configuration conf) throws IOException {
    if (Configurable.class.isAssignableFrom(inputFormatClass) ||
        JobConfigurable.class.isAssignableFrom(inputFormatClass)) {
      return ReflectionUtil.newInstance(inputFormatClass, conf);
    }
    // TODO: why is this copy-pasted from HiveInputFormat?
    InputFormat format = inputFormats.get(inputFormatClass.getName());

    if (format == null) {
      format = newInputFormat(inputFormatClass, conf);
    }
    if (CacheableInputFormat.class.isAssignableFrom(inputFormatClass)) {
      format = ((CacheableInputFormat<?, ?>)format).newInstance(format);
    }
    return format;
  }

  private static InputFormat<?, ?> newInputFormat(Class<? extends InputFormat> inputFormatClass,
      Configuration conf)
      throws IOException {
    InputFormat<?, ?> format;
    try {
      format = ReflectionUtil.newInstance(inputFormatClass, conf);
      // HBase input formats are not thread safe today. See HIVE-8808.
      String inputFormatName = inputFormatClass.getName().toLowerCase();
      if (!inputFormatName.contains("hbase")) {
        inputFormats.put(inputFormatClass.getName(), format);
      }
    } catch (Exception e) {
      throw new IOException("Cannot create an instance of InputFormat class: " + inputFormatClass.getName(), e);
    }
    return format;
  }
}
