package org.apache.hadoop.hive.registry.serdes.avro;


import org.apache.avro.Schema;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Interface for serializing and deserializing avro payloads.
 */
public interface AvroSerDesHandler {

  void handlePayloadSerialization(OutputStream outputStream, Object input);

  Object handlePayloadDeserialization(InputStream payloadInputStream,
                                      Schema writerSchema,
                                      Schema readerSchema,
                                      boolean useSpecificAvroReader);
}
