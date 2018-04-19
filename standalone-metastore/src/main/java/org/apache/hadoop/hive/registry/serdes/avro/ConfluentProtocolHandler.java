package org.apache.hadoop.hive.registry.serdes.avro;

import org.apache.hadoop.hive.registry.SchemaIdVersion;
import org.apache.hadoop.hive.registry.serdes.SerDesProtocolHandlerRegistry;
import org.apache.hadoop.hive.registry.serdes.avro.exceptions.AvroException;
import org.apache.hadoop.hive.registry.serdes.avro.exceptions.AvroRetryableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Confluent compatible protocol handler.
 */
public class ConfluentProtocolHandler extends AbstractAvroSerDesProtocolHandler {

  public static final Logger LOG = LoggerFactory.getLogger(ConfluentProtocolHandler.class);

  public ConfluentProtocolHandler() {
    super(SerDesProtocolHandlerRegistry.CONFLUENT_VERSION_PROTOCOL, new ConfluentAvroSerDesHandler());
  }

  @Override
  protected void doHandleSchemaVersionSerialization(OutputStream outputStream,
                                                    SchemaIdVersion schemaIdVersion) {
    Long versionId = schemaIdVersion.getSchemaVersionId();
    if (versionId > Integer.MAX_VALUE) {
      throw new AvroException("Unsupported versionId, max id=" + Integer.MAX_VALUE + " , but was id=" + versionId);
    } else {
      // 4 bytes
      try {
        outputStream.write(ByteBuffer.allocate(4)
                .putInt(versionId.intValue()).array());
      } catch (IOException e) {
        throw new AvroRetryableException(e);
      }
    }
  }

  @Override
  public SchemaIdVersion handleSchemaVersionDeserialization(InputStream inputStream) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(4);
    try {
      inputStream.read(byteBuffer.array());
    } catch (IOException e) {
      throw new AvroRetryableException(e);
    }

    int schemaVersionId = byteBuffer.getInt();
    return new SchemaIdVersion((long) schemaVersionId);
  }

}

