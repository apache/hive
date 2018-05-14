package org.apache.hadoop.hive.registry.serdes.avro;

import org.apache.hadoop.hive.registry.SchemaIdVersion;
import org.apache.hadoop.hive.registry.serdes.SerDesException;
import org.apache.hadoop.hive.registry.serdes.SerDesProtocolHandlerRegistry;
import org.apache.hadoop.hive.registry.serdes.avro.exceptions.AvroRetryableException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class SchemaVersionIdAsLongProtocolHandler extends AbstractAvroSerDesProtocolHandler {

  public SchemaVersionIdAsLongProtocolHandler() {
    super(SerDesProtocolHandlerRegistry.VERSION_ID_AS_LONG_PROTOCOL, new DefaultAvroSerDesHandler());
  }

  @Override
  public void doHandleSchemaVersionSerialization(OutputStream outputStream,
                                                 SchemaIdVersion schemaIdVersion) throws SerDesException {
    try {
      Long versionId = schemaIdVersion.getSchemaVersionId();
      outputStream.write(ByteBuffer.allocate(8)
              .putLong(versionId).array());
    } catch (IOException e) {
      throw new AvroRetryableException(e);
    }
  }

  @Override
  public SchemaIdVersion handleSchemaVersionDeserialization(InputStream inputStream) throws SerDesException  {
    ByteBuffer byteBuffer = ByteBuffer.allocate(8);
    try {
      inputStream.read(byteBuffer.array());
    } catch (IOException e) {
      throw new AvroRetryableException(e);
    }

    return new SchemaIdVersion(byteBuffer.getLong());
  }

  public Byte getProtocolId() {
    return protocolId;
  }
}
