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
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

/**
 * Utilities useful only to the AvroSerde itself.  Not mean to be used by
 * end-users but public for interop to the ql package.
 */
public class AvroSerdeUtils {
  private static final Log LOG = LogFactory.getLog(AvroSerdeUtils.class);

  /**
   * Enum container for all avro table properties.
   * If introducing a new avro-specific table property,
   * add it here. Putting them in an enum rather than separate strings
   * allows them to be programmatically grouped and referenced together.
   */
  public static enum AvroTableProperties {
    SCHEMA_LITERAL("avro.schema.literal"),
    SCHEMA_URL("avro.schema.url"),
    SCHEMA_NAMESPACE("avro.schema.namespace"),
    SCHEMA_NAME("avro.schema.name"),
    SCHEMA_DOC("avro.schema.doc"),
    AVRO_SERDE_SCHEMA("avro.serde.schema"),
    SCHEMA_RETRIEVER("avro.schema.retriever");

    private final String propName;

    AvroTableProperties(String propName) {
      this.propName = propName;
    }

    public String getPropName(){
      return this.propName;
    }
  }

  // Following parameters slated for removal, prefer usage of enum above, that allows programmatic access.
  @Deprecated public static final String SCHEMA_LITERAL = "avro.schema.literal";
  @Deprecated public static final String SCHEMA_URL = "avro.schema.url";
  @Deprecated public static final String SCHEMA_NAMESPACE = "avro.schema.namespace";
  @Deprecated public static final String SCHEMA_NAME = "avro.schema.name";
  @Deprecated public static final String SCHEMA_DOC = "avro.schema.doc";
  @Deprecated public static final String AVRO_SERDE_SCHEMA = AvroTableProperties.AVRO_SERDE_SCHEMA.getPropName();
  @Deprecated public static final String SCHEMA_RETRIEVER = AvroTableProperties.SCHEMA_RETRIEVER.getPropName();

  public static final String SCHEMA_NONE = "none";
  public static final String EXCEPTION_MESSAGE = "Neither "
      + AvroTableProperties.SCHEMA_LITERAL.getPropName() + " nor "
      + AvroTableProperties.SCHEMA_URL.getPropName() + " specified, can't determine table schema";



  /**
   * Determine the schema to that's been provided for Avro serde work.
   * @param properties containing a key pointing to the schema, one way or another
   * @return schema to use while serdeing the avro file
   * @throws IOException if error while trying to read the schema from another location
   * @throws AvroSerdeException if unable to find a schema or pointer to it in the properties
   */
  public static Schema determineSchemaOrThrowException(Configuration conf, Properties properties)
          throws IOException, AvroSerdeException {
    String schemaString = properties.getProperty(AvroTableProperties.SCHEMA_LITERAL.getPropName());
    if(schemaString != null && !schemaString.equals(SCHEMA_NONE))
      return AvroSerdeUtils.getSchemaFor(schemaString);

    // Try pulling directly from URL
    schemaString = properties.getProperty(AvroTableProperties.SCHEMA_URL.getPropName());
    if(schemaString == null || schemaString.equals(SCHEMA_NONE))
      throw new AvroSerdeException(EXCEPTION_MESSAGE);

    try {
      Schema s = getSchemaFromFS(schemaString, conf);
      if (s == null) {
        //in case schema is not a file system
        return AvroSerdeUtils.getSchemaFor(new URL(schemaString).openStream());
      }
      return s;
    } catch (IOException ioe) {
      throw new AvroSerdeException("Unable to read schema from given path: " + schemaString, ioe);
    } catch (URISyntaxException urie) {
      throw new AvroSerdeException("Unable to read schema from given path: " + schemaString, urie);
    }
  }

  // Protected for testing and so we can pass in a conf for testing.
  protected static Schema getSchemaFromFS(String schemaFSUrl,
                          Configuration conf) throws IOException, URISyntaxException {
    FSDataInputStream in = null;
    FileSystem fs = null;
    try {
      fs = FileSystem.get(new URI(schemaFSUrl), conf);
    } catch (IOException ioe) {
      //return null only if the file system in schema is not recognized
      String msg = "Failed to open file system for uri " + schemaFSUrl + " assuming it is not a FileSystem url";
      LOG.debug(msg, ioe);
      return null;
    }
    try {
      in = fs.open(new Path(schemaFSUrl));
      Schema s = AvroSerdeUtils.getSchemaFor(in);
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

  public static Buffer getBufferFromBytes(byte[] input) {
    ByteBuffer bb = ByteBuffer.wrap(input);
    return bb.rewind();
  }

  public static Buffer getBufferFromDecimal(HiveDecimal dec, int scale) {
    if (dec == null) {
      return null;
    }

    dec = dec.setScale(scale);
    return AvroSerdeUtils.getBufferFromBytes(dec.unscaledValue().toByteArray());
  }

  public static byte[] getBytesFromByteBuffer(ByteBuffer byteBuffer) {
    byteBuffer.rewind();
    byte[] result = new byte[byteBuffer.limit()];
    byteBuffer.get(result);
    return result;
  }

  public static HiveDecimal getHiveDecimalFromByteBuffer(ByteBuffer byteBuffer, int scale) {
    byte[] result = getBytesFromByteBuffer(byteBuffer);
    HiveDecimal dec = HiveDecimal.create(new BigInteger(result), scale);
    return dec;
  }

  public static Schema getSchemaFor(String str) {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(str);
    return schema;
  }

  public static Schema getSchemaFor(File file) {
    Schema.Parser parser = new Schema.Parser();
    Schema schema;
    try {
      schema = parser.parse(file);
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse Avro schema from " + file.getName(), e);
    }
    return schema;
  }

  public static Schema getSchemaFor(InputStream stream) {
    Schema.Parser parser = new Schema.Parser();
    Schema schema;
    try {
      schema = parser.parse(stream);
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse Avro schema", e);
    }
    return schema;
  }
}
