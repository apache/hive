package org.apache.hadoop.hive.registry.serdes.avro;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.registry.SchemaIdVersion;
import org.apache.hadoop.hive.registry.SchemaMetadata;
import org.apache.hadoop.hive.registry.SchemaVersionInfo;
import org.apache.hadoop.hive.registry.SchemaVersionKey;
import org.apache.hadoop.hive.registry.SchemaVersionRetriever;
import org.apache.hadoop.hive.registry.client.ISchemaRegistryClient;
import org.apache.hadoop.hive.registry.common.errors.InvalidSchemaException;
import org.apache.hadoop.hive.registry.common.errors.SchemaNotFoundException;
import org.apache.hadoop.hive.registry.common.exception.RegistryException;
import org.apache.hadoop.hive.registry.serdes.AbstractSnapshotDeserializer;
import org.apache.hadoop.hive.registry.serdes.SerDesException;
import org.apache.hadoop.hive.registry.serdes.SerDesProtocolHandler;
import org.apache.hadoop.hive.registry.serdes.SerDesProtocolHandlerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hive.registry.serdes.avro.AbstractAvroSerDesProtocolHandler.READER_SCHEMA;
import static org.apache.hadoop.hive.registry.serdes.avro.AbstractAvroSerDesProtocolHandler.WRITER_SCHEMA;

/**
 * This class implements most of the required functionality for an avro deserializer by extending {@link AbstractSnapshotDeserializer}
 * and implementing the required methods.
 * <p>
 * <p>
 * The below example describes how to extend this deserializer with user supplied representation like MessageContext.
 * Default deserialization of avro payload is implemented in {@link #buildDeserializedObject(byte, InputStream, SchemaMetadata, Integer, Integer)}
 * and it can be used while implementing {@link #doDeserialize(Object, byte, SchemaMetadata, Integer, Integer)} as given
 * below.
 * </p>
 * <p>
 * <pre>{@code
 * public class MessageContext {
 * final Map<String, Object> headers;
 * final InputStream payloadEntity;
 *
 * public MessageContext(Map<String, Object> headers, InputStream payloadEntity) {
 * this.headers = headers;
 * this.payloadEntity = payloadEntity;
 * }
 * }
 *
 * public class MessageContextBasedDeserializer extends AbstractAvroSnapshotDeserializer<MessageContext> {
 *
 * {@literal @}Override
 * protected Object doDeserialize(MessageContext input,
 * byte protocolId,
 * SchemaMetadata schemaMetadata,
 * Integer writerSchemaVersion,
 * Integer readerSchemaVersion) throws SerDesException {
 * return buildDeserializedObject(protocolId,
 * input.payloadEntity,
 * schemaMetadata,
 * writerSchemaVersion,
 * readerSchemaVersion);
 * }
 *
 * {@literal @}Override
 * protected byte retrieveProtocolId(MessageContext input) throws SerDesException {
 * return (byte) input.headers.get("protocol.id");
 * }
 *
 * {@literal @}Override
 * protected SchemaIdVersion retrieveSchemaIdVersion(byte protocolId, MessageContext input) throws SerDesException {
 * Long id = (Long) input.headers.get("schema.metadata.id");
 * Integer version = (Integer) input.headers.get("schema.version");
 * return new SchemaIdVersion(id, version);
 * }
 * }
 *
 * }</pre>
 *
 * @param <I> representation of the received input payload
 */
public abstract class AbstractAvroSnapshotDeserializer<I> extends AbstractSnapshotDeserializer<I, Object, Schema> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractAvroSnapshotDeserializer.class);

  public static final String SPECIFIC_AVRO_READER = "specific.avro.reader";

  private AvroSchemaResolver avroSchemaResolver;

  protected boolean useSpecificAvroReader = false;

  public AbstractAvroSnapshotDeserializer() {
    super();
  }

  public AbstractAvroSnapshotDeserializer(ISchemaRegistryClient schemaRegistryClient) {
    super(schemaRegistryClient);
  }

  @Override
  public void doInit(Map<String, ?> config) {
    super.doInit(config);
    SchemaVersionRetriever schemaVersionRetriever = createSchemaVersionRetriever();
    avroSchemaResolver = new AvroSchemaResolver(schemaVersionRetriever);
    useSpecificAvroReader = (boolean) getValue(config, SPECIFIC_AVRO_READER, false);
  }

  private SchemaVersionRetriever createSchemaVersionRetriever() {
    return new SchemaVersionRetriever() {
      @Override
      public SchemaVersionInfo retrieveSchemaVersion(SchemaVersionKey key) throws SchemaNotFoundException {
        return schemaRegistryClient.getSchemaVersionInfo(key);
      }

      @Override
      public SchemaVersionInfo retrieveSchemaVersion(SchemaIdVersion key) throws SchemaNotFoundException {
        return schemaRegistryClient.getSchemaVersionInfo(key);
      }
    };
  }

  @Override
  protected Schema getParsedSchema(SchemaVersionKey schemaVersionKey) throws InvalidSchemaException, SchemaNotFoundException {
    return new Schema.Parser().parse(avroSchemaResolver.resolveSchema(schemaVersionKey));
  }

  /**
   * Builds the deserialized object from the given {@code payloadInputStream} and applying writer and reader schemas
   * from the respective given versions.
   *
   * @param protocolId          protocol id
   * @param payloadInputStream  payload
   * @param schemaMetadata      metadata about schema
   * @param writerSchemaVersion schema version of the writer
   * @param readerSchemaVersion schema version to be applied for reading or projection
   * @return the deserialized object
   * @throws SerDesException when any ser/des error occurs
   */
  protected Object buildDeserializedObject(byte protocolId,
                                           InputStream payloadInputStream,
                                           SchemaMetadata schemaMetadata,
                                           Integer writerSchemaVersion,
                                           Integer readerSchemaVersion) throws SerDesException {
    Object deserializedObj;
    String schemaName = schemaMetadata.getName();
    SchemaVersionKey writerSchemaVersionKey = new SchemaVersionKey(schemaName, writerSchemaVersion);
    LOG.debug("SchemaKey: [{}] for the received payload", writerSchemaVersionKey);
    Schema writerSchema = getSchema(writerSchemaVersionKey);
    if (writerSchema == null) {
      throw new RegistryException("No schema exists with metadata-key: " + schemaMetadata + " and writerSchemaVersion: " + writerSchemaVersion);
    }
    Schema readerSchema = readerSchemaVersion != null ? getSchema(new SchemaVersionKey(schemaName, readerSchemaVersion)) : null;

    deserializedObj = deserializePayloadForProtocol(protocolId, payloadInputStream, writerSchema, readerSchema);

    return deserializedObj;
  }

  protected Object deserializePayloadForProtocol(byte protocolId,
                                                 InputStream payloadInputStream,
                                                 Schema writerSchema,
                                                 Schema readerSchema) throws SerDesException  {
    Map<String, Object> props = new HashMap<>();
    props.put(SPECIFIC_AVRO_READER, useSpecificAvroReader);
    props.put(WRITER_SCHEMA, writerSchema);
    props.put(READER_SCHEMA, readerSchema);
    SerDesProtocolHandler serDesProtocolHandler = SerDesProtocolHandlerRegistry.get().getSerDesProtocolHandler(protocolId);

    return serDesProtocolHandler.handlePayloadDeserialization(payloadInputStream, props);
  }
}
