/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.serde2.avro;

import org.apache.avro.Schema;

/**
 * Retrieves the avro schema from the given source. "Source" is a little loose term here in the
 * sense it can range from being an HDFS url location pointing to the schema or it can be even as
 * simple as a {@link java.util.Properties properties} file with a simple key-value mapping to the schema. For
 * cases where the {@link Schema schema} is a part of the serialized data itself, "Source" would
 * refer to the data bytes from which the {@link Schema schema} has to retrieved.
 *
 * */
public abstract class AvroSchemaRetriever {

  /**
   * Retrieve the writer avro schema from the given source
   *
   * @param source source from which the schema has to retrieved
   * @return the retrieved writer {@link Schema}
   * */
  public abstract Schema retrieveWriterSchema(Object source);

  /**
   * Retrieve the reader avro schema from the given source
   *
   * @param source source from which the schema has to retrieved
   * @return the retrieved reader {@link Schema}
   * */
  public Schema retrieveReaderSchema(Object source) {
    return null;
  }

  /**
   * Possible offset associated with schema. This is useful when the schema is stored inline along
   * with the data.
   *
   * <p>
   * Defaulted to zero. Consumers can choose to override this value to provide a custom offset.
   * </p>
   * */
  public int getOffset() {
    return 0;
  }
}