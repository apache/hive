package org.apache.hadoop.hive.registry.serdes.avro;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.registry.SchemaIdVersion;
import org.apache.hadoop.hive.registry.serdes.SerDesProtocolHandler;
import org.apache.hadoop.hive.registry.serdes.avro.exceptions.AvroRetryableException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import static org.apache.hadoop.hive.registry.serdes.avro.AbstractAvroSnapshotDeserializer.SPECIFIC_AVRO_READER;

public abstract class AbstractAvroSerDesProtocolHandler implements SerDesProtocolHandler {

  /**
   * Property name for writer schema
   */
  public static final String WRITER_SCHEMA = "writer.schema";

  /**
   * Property name for reader schema
   */
  public static final String READER_SCHEMA = "reader.schema";

  private final AvroSerDesHandler avroSerDesHandler;

  protected final Byte protocolId;

  protected AbstractAvroSerDesProtocolHandler(Byte protocolId, AvroSerDesHandler avroSerDesHandler) {
    this.protocolId = protocolId;
    this.avroSerDesHandler = avroSerDesHandler;
  }

  @Override
  public void handleSchemaVersionSerialization(OutputStream outputStream, SchemaIdVersion schemaIdVersion) {
    try {
      outputStream.write(new byte[]{protocolId});
      doHandleSchemaVersionSerialization(outputStream, schemaIdVersion);
    } catch (IOException e) {
      throw new AvroRetryableException(e);
    }
  }

  protected abstract void doHandleSchemaVersionSerialization(OutputStream outputStream, SchemaIdVersion schemaIdVersion)
          throws IOException;

  @Override
  public Byte getProtocolId() {
    return protocolId;
  }

  @Override
  public void handlePayloadSerialization(OutputStream outputStream, Object input) {
    avroSerDesHandler.handlePayloadSerialization(outputStream, input);
  }

  @Override
  public Object handlePayloadDeserialization(InputStream payloadInputStream, Map<String, Object> context) {
    boolean useSpecificAvroReader = (boolean) context.getOrDefault(SPECIFIC_AVRO_READER, false);
    Schema writerSchema = (Schema) context.get(WRITER_SCHEMA);
    Schema readerSchema = (Schema) context.get(READER_SCHEMA);
    return avroSerDesHandler.handlePayloadDeserialization(payloadInputStream,
            writerSchema,
            readerSchema,
            useSpecificAvroReader);
  }
}

