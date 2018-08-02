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

public class SchemaVersionIdAsIntProtocolHandler extends AbstractAvroSerDesProtocolHandler {
  public static final Logger LOG = LoggerFactory.getLogger(SchemaVersionIdAsIntProtocolHandler.class);

  private final SchemaVersionIdAsLongProtocolHandler delegate;

  public SchemaVersionIdAsIntProtocolHandler() {
    super(SerDesProtocolHandlerRegistry.VERSION_ID_AS_INT_PROTOCOL, new DefaultAvroSerDesHandler());
    delegate = new SchemaVersionIdAsLongProtocolHandler();
  }

  @Override
  public void handleSchemaVersionSerialization(OutputStream outputStream,
                                               SchemaIdVersion schemaIdVersion) {
    Long versionId = schemaIdVersion.getSchemaVersionId();
    if (versionId > Integer.MAX_VALUE) {
      // if it is more than int max, fallback to SchemaVersionIdAsLongProtocolHandler
      LOG.debug("Upgraded to " + delegate + " as versionId is more than max integer");
      delegate.handleSchemaVersionSerialization(outputStream, schemaIdVersion);
    } else {
      // 4 bytes
      try {
        outputStream.write(new byte[]{protocolId});
        outputStream.write(ByteBuffer.allocate(4)
                .putInt(versionId.intValue()).array());
      } catch (IOException e) {
        throw new AvroException(e);
      }
    }
  }

  @Override
  protected void doHandleSchemaVersionSerialization(OutputStream outputStream, SchemaIdVersion schemaIdVersion) throws IOException {
    // ignore this as this would never be invoked.
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

  public Byte getProtocolId() {
    return protocolId;
  }
}
