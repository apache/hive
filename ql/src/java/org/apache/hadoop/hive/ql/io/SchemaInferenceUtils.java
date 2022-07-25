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
package org.apache.hadoop.hive.ql.io;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SchemaInference;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.List;

public class SchemaInferenceUtils {
  private static Class<AbstractSerDe> getSerde(Configuration conf, String fileFormat) throws HiveException {
    StorageFormatFactory storageFormatFactory = new StorageFormatFactory();
    StorageFormatDescriptor descriptor = storageFormatFactory.get(fileFormat);
    if (descriptor == null) {
      throw new HiveException(ErrorMsg.CTLF_MISSING_STORAGE_FORMAT_DESCRIPTOR.getErrorCodedMsg(fileFormat));
    }
    String serde = descriptor.getSerde();
    try {
      return (Class<AbstractSerDe>) conf.getClassByName(serde);
    } catch (ClassNotFoundException e) {
      throw new HiveException(ErrorMsg.CTLF_CLASS_NOT_FOUND.getErrorCodedMsg(serde, fileFormat), e);
    }
  }

  /**
   * Determines if a supplied fileFormat supports Schema Inference for CREATE TABLE LIKE FILE.
   *
   * @param conf Configuration object used to get class.
   * @param fileFormat File format to check for Schema Inference support.
   * @throws HiveException if unable to get SerDe class for fileFormat
   */
  public static boolean doesSupportSchemaInference(Configuration conf, String fileFormat) throws HiveException {
    return SchemaInference.class.isAssignableFrom(getSerde(conf, fileFormat));
  }

  /**
   * Returns a List containing FieldSchema as determined by the readSchema method of the provided file format.
   *
   * @param conf Hadoop Configuration object used to look up class and provided to the readSchema method.
   * @param fileFormat File format in which to use SerDe from.
   * @param filePath Path to the file to read.
   * @throws HiveException if unable to read the schema
   */
  public static List<FieldSchema> readSchemaFromFile(Configuration conf, String fileFormat, String filePath)
          throws HiveException {
    Class<AbstractSerDe> asClass = getSerde(conf, fileFormat);
    SchemaInference sd = (SchemaInference) ReflectionUtils.newInstance(asClass, conf);
    try {
      return sd.readSchema(conf, filePath);
    } catch (SerDeException e) {
      throw new HiveException(ErrorMsg.CTLF_FAILED_INFERENCE.getErrorCodedMsg(), e);
    }
  }
}
