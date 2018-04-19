package org.apache.hadoop.hive.registry.serdes.avro;

import org.apache.hadoop.hive.registry.SchemaIdVersion;
import org.apache.hadoop.hive.registry.serdes.SerDesException;
import org.apache.hadoop.hive.registry.serdes.SerDesProtocolHandlerRegistry;
import org.apache.hadoop.hive.registry.serdes.avro.exceptions.AvroRetryableException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class SchemaMetadataIdProtocolHandler extends AbstractAvroSerDesProtocolHandler {

  public SchemaMetadataIdProtocolHandler() {
    super(SerDesProtocolHandlerRegistry.METADATA_ID_VERSION_PROTOCOL, new DefaultAvroSerDesHandler());
  }

  @Override
  protected void doHandleSchemaVersionSerialization(OutputStream outputStream,
                                                    SchemaIdVersion schemaIdVersion) throws SerDesException {
    // 8 bytes : schema metadata Id
    // 4 bytes : schema version
    try {
      outputStream.write(ByteBuffer.allocate(12)
              .putLong(schemaIdVersion.getSchemaMetadataId())
              .putInt(schemaIdVersion.getVersion()).array());
    } catch (IOException e) {
      throw new AvroRetryableException(e);
    }
  }

  @Override
  public SchemaIdVersion handleSchemaVersionDeserialization(InputStream inputStream) {
    // 8 bytes : schema metadata Id
    // 4 bytes : schema version
    ByteBuffer byteBuffer = ByteBuffer.allocate(12);
    try {
      inputStream.read(byteBuffer.array());
    } catch (IOException e) {
      throw new AvroRetryableException(e);
    }

    long schemaMetadataId = byteBuffer.getLong();
    int schemaVersion = byteBuffer.getInt();

    return new SchemaIdVersion(schemaMetadataId, schemaVersion);
  }

}
