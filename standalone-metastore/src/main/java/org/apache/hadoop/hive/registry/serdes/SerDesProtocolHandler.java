package org.apache.hadoop.hive.registry.serdes;

import org.apache.hadoop.hive.registry.SchemaIdVersion;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

/**
 * This class allows users to customize how schema version identifiers and actual payload are sent/received as part
 * of ser/des.
 */
public interface SerDesProtocolHandler {

  /**
   * @return protocol id for this handler.
   */
  Byte getProtocolId();

  /**
   * Serializes protocol id and schema version related information into the given output stream.
   *
   * @param outputStream    output stream
   * @param schemaIdVersion schema version info to be serialized
   */
  void handleSchemaVersionSerialization(OutputStream outputStream, SchemaIdVersion schemaIdVersion) throws SerDesException;

  /**
   * Deserializes schema version related information from the given input stream.
   *
   * @param inputStream input stream
   * @return {@link SchemaIdVersion} instenace created from deserializing respective information from given input stream.
   */
  SchemaIdVersion handleSchemaVersionDeserialization(InputStream inputStream) throws SerDesException;

  /**
   * Handles serialization of input into given output stream
   *
   * @param outputStream output stream
   * @param input        object to be serialized
   */
  void handlePayloadSerialization(OutputStream outputStream, Object input) throws SerDesException;

  /**
   * Handles deserialization of given input stream and returns the deserialized Object.
   *
   * @param inputStream input stream
   * @param context     any context required for deserialization.
   * @return returns the deserialized Object.
   */
  Object handlePayloadDeserialization(InputStream inputStream, Map<String, Object> context) throws SerDesException;

}
