package org.apache.hadoop.hive.registry.serdes.avro;

import org.apache.hadoop.hive.registry.SchemaIdVersion;
import org.apache.hadoop.hive.registry.SchemaMetadata;
import org.apache.hadoop.hive.registry.client.ISchemaRegistryClient;
import org.apache.hadoop.hive.registry.serdes.SerDesException;
import org.apache.hadoop.hive.registry.serdes.avro.exceptions.AvroException;
import org.apache.hadoop.hive.registry.serdes.avro.exceptions.AvroRetryableException;
import org.apache.hadoop.hive.registry.serdes.SerDesProtocolHandlerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;


/**
 * This is the default implementation of {@link AbstractAvroSnapshotDeserializer}.
 */
public class AvroSnapshotDeserializer extends AbstractAvroSnapshotDeserializer<InputStream> {
    private static final Logger LOG = LoggerFactory.getLogger(AvroSnapshotDeserializer.class);

    public AvroSnapshotDeserializer() {
    }

    public AvroSnapshotDeserializer(ISchemaRegistryClient schemaRegistryClient) {
        super(schemaRegistryClient);
    }

    protected SchemaIdVersion retrieveSchemaIdVersion(byte protocolId, InputStream inputStream)
            throws SerDesException {
        return SerDesProtocolHandlerRegistry.get()
                                            .getSerDesProtocolHandler(protocolId)
                                            .handleSchemaVersionDeserialization(inputStream);
    }

    protected byte retrieveProtocolId(InputStream inputStream) throws SerDesException {
        // first byte is protocol version/id.
        // protocol format:
        // 1 byte  : protocol version
        byte protocolId;
        try {
            protocolId = (byte) inputStream.read();
        } catch (IOException e) {
            throw new AvroRetryableException(e);
        }

        if (protocolId == -1) {
            throw new AvroException("End of stream reached while trying to read protocol id");
        }

        checkProtocolHandlerExists(protocolId);

        return protocolId;
    }

    private void checkProtocolHandlerExists(byte protocolId) {
        if (SerDesProtocolHandlerRegistry.get().getSerDesProtocolHandler(protocolId) == null) {
            throw new AvroException("Unknown protocol id [" + protocolId + "] received while deserializing the payload");
        }
    }

    protected Object doDeserialize(InputStream payloadInputStream,
                                   byte protocolId,
                                   SchemaMetadata schemaMetadata,
                                   Integer writerSchemaVersion,
                                   Integer readerSchemaVersion) throws SerDesException {

        return buildDeserializedObject(protocolId, payloadInputStream, schemaMetadata, writerSchemaVersion, readerSchemaVersion);
    }
    
    
}