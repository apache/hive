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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Properties;

/**
 * Utilities useful only to the AvroSerde itself.  Not mean to be used by
 * end-users but public for interop to the ql package.
 */
public class AvroSerdeUtils {
  private static final Log LOG = LogFactory.getLog(AvroSerdeUtils.class);

  public static final String SCHEMA_LITERAL = "avro.schema.literal";
  public static final String SCHEMA_URL = "avro.schema.url";
  public static final String SCHEMA_NONE = "none";
  public static final String EXCEPTION_MESSAGE = "Neither " + SCHEMA_LITERAL + " nor "
          + SCHEMA_URL + " specified, can't determine table schema";
  public static final String AVRO_SERDE_SCHEMA = "avro.serde.schema";

  /**
   * Determine the schema to that's been provided for Avro serde work.
   * @param properties containing a key pointing to the schema, one way or another
   * @return schema to use while serdeing the avro file
   * @throws IOException if error while trying to read the schema from another location
   * @throws AvroSerdeException if unable to find a schema or pointer to it in the properties
   */
  public static Schema determineSchemaOrThrowException(Properties properties)
          throws IOException, AvroSerdeException {
    String schemaString = properties.getProperty(SCHEMA_LITERAL);
    if(schemaString != null && !schemaString.equals(SCHEMA_NONE))
      return Schema.parse(schemaString);

    // Try pulling directly from URL
    schemaString = properties.getProperty(SCHEMA_URL);
    if(schemaString == null || schemaString.equals(SCHEMA_NONE))
      throw new AvroSerdeException(EXCEPTION_MESSAGE);

    try {
      if(schemaString.toLowerCase().startsWith("hdfs://"))
        return getSchemaFromHDFS(schemaString, new Configuration());
    } catch(IOException ioe) {
      throw new AvroSerdeException("Unable to read schema from HDFS: " + schemaString, ioe);
    }

    return Schema.parse(new URL(schemaString).openStream());
  }

  /**
   * Attempt to determine the schema via the usual means, but do not throw
   * an exception if we fail.  Instead, signal failure via a special
   * schema.  This is used because Hive calls init on the serde during
   * any call, including calls to update the serde properties, meaning
   * if the serde is in a bad state, there is no way to update that state.
   */
  public static Schema determineSchemaOrReturnErrorSchema(Properties props) {
    try {
      return determineSchemaOrThrowException(props);
    } catch(AvroSerdeException he) {
      LOG.warn("Encountered AvroSerdeException determining schema. Returning " +
              "signal schema to indicate problem", he);
      return SchemaResolutionProblem.SIGNAL_BAD_SCHEMA;
    } catch (Exception e) {
      LOG.warn("Encountered exception determining schema. Returning signal " +
              "schema to indicate problem", e);
      return SchemaResolutionProblem.SIGNAL_BAD_SCHEMA;
    }
  }
  // Protected for testing and so we can pass in a conf for testing.
  protected static Schema getSchemaFromHDFS(String schemaHDFSUrl,
                                            Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FSDataInputStream in = null;

    try {
      in = fs.open(new Path(schemaHDFSUrl));
      Schema s = Schema.parse(in);
      return s;
    } finally {
      if(in != null) in.close();
    }
  }

  /**
   * Determine if an Avro schema is of type Union[T, NULL].  Avro supports nullable
   * types via a union of type T and null.  This is a very common use case.
   * As such, we want to silently convert it to just T and allow the value to be null.
   *
   * @return true if type represents Union[T, Null], false otherwise
   */
  public static boolean isNullableType(Schema schema) {
    return schema.getType().equals(Schema.Type.UNION) &&
           schema.getTypes().size() == 2 &&
             (schema.getTypes().get(0).getType().equals(Schema.Type.NULL) ||
              schema.getTypes().get(1).getType().equals(Schema.Type.NULL));
      // [null, null] not allowed, so this check is ok.
  }

  /**
   * In a nullable type, get the schema for the non-nullable type.  This method
   * does no checking that the provides Schema is nullable.
   */
  public static Schema getOtherTypeFromNullableType(Schema schema) {
    List<Schema> types = schema.getTypes();

    return types.get(0).getType().equals(Schema.Type.NULL) ? types.get(1) : types.get(0);
  }

  /**
   * Determine if we're being executed from within an MR job or as part
   * of a select * statement.  The signals for this varies between Hive versions.
   * @param job that contains things that are or are not set in a job
   * @return Are we in a job or not?
   */
  public static boolean insideMRJob(JobConf job) {
    return job != null
           && (HiveConf.getVar(job, HiveConf.ConfVars.PLAN) != null)
           && (!HiveConf.getVar(job, HiveConf.ConfVars.PLAN).isEmpty());
  }
}
