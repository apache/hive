package org.apache.hadoop.hive.registry.serdes.avro;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.registry.SchemaIdVersion;
import org.apache.hadoop.hive.registry.client.ISchemaRegistryClient;
import org.apache.hadoop.hive.registry.serdes.AbstractSnapshotSerializer;
import org.apache.hadoop.hive.registry.serdes.SerDesException;
import org.apache.hadoop.hive.registry.serdes.SerDesProtocolHandler;
import org.apache.hadoop.hive.registry.serdes.SerDesProtocolHandlerRegistry;
import org.apache.hadoop.hive.registry.serdes.avro.exceptions.AvroException;

import java.io.OutputStream;
import java.util.Map;

/**
 * The below example describes how to extend this serializer with user supplied representation like MessageContext class.
 * Respective {@link SnapshotDeserializer} implementation is done extending {@link AbstractSnapshotDeserializer}.
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
 * public class MessageContextBasedAvroSerializer extends AbstractAvroSnapshotSerializer<MessageContext> {
 *
 * {@literal @}Override
 * protected MessageContext doSerialize(Object input, SchemaIdVersion schemaIdVersion) throws SerDesException {
 * Map<String, Object> headers = new HashMap<>();
 *
 * headers.put("protocol.id", 0x1);
 * headers.put("schema.metadata.id", schemaIdVersion.getSchemaMetadataId());
 * headers.put("schema.version", schemaIdVersion.getVersion());
 *
 * ByteArrayOutputStream baos = new ByteArrayOutputStream();
 *
 * try(BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(baos)) {
 * writeContentPayload(input, bufferedOutputStream);
 * } catch (IOException e) {
 * throw new SerDesException(e);
 * }
 *
 * ByteArrayInputStream payload = new ByteArrayInputStream(baos.toByteArray());
 *
 * return new MessageContext(headers, payload);
 * }
 * }
 * }</pre>
 *
 * @param <O> serialized output type. For ex: byte[], String etc.
 */
public abstract class AbstractAvroSnapshotSerializer<O> extends AbstractSnapshotSerializer<Object, O> {

  public AbstractAvroSnapshotSerializer() {
    super();
  }

  public AbstractAvroSnapshotSerializer(ISchemaRegistryClient schemaRegistryClient) {
    super(schemaRegistryClient);
  }

  /**
   * Property name for protocol version to be set with {@link #init(Map)}.
   */
  public static final String SERDES_PROTOCOL_VERSION = "serdes.protocol.version";

  protected SerDesProtocolHandler serDesProtocolHandler;

  @Override
  public void doInit(Map<String, ?> config) {

    Number number = (Number) ((Map<String, Object>) config).getOrDefault(SERDES_PROTOCOL_VERSION,
            SerDesProtocolHandlerRegistry.CURRENT_PROTOCOL);
    validateSerdesProtocolVersion(number);

    Byte protocolVersion = number.byteValue();

    SerDesProtocolHandler serDesProtocolHandler = SerDesProtocolHandlerRegistry.get().getSerDesProtocolHandler(protocolVersion);
    if (serDesProtocolHandler == null) {
      throw new AvroException("SerDesProtocolHandler with protocol version " + protocolVersion + " does not exist");
    }

    this.serDesProtocolHandler = serDesProtocolHandler;
  }

  private void validateSerdesProtocolVersion(Number number) {
    final long x;
    if ((x = number.longValue()) != number.doubleValue()
            || (x < 0 || x > Byte.MAX_VALUE)) {
      throw new AvroException(SERDES_PROTOCOL_VERSION + " value should be in [0, " + Byte.MAX_VALUE + "]");
    }
  }

  /**
   * @param input avro object
   * @return textual representation of the schema of the given {@code input} avro object
   */
  protected String getSchemaText(Object input) {
    Schema schema = AvroUtils.computeSchema(input);
    return schema.toString();
  }

  protected void serializeSchemaVersion(OutputStream os, SchemaIdVersion schemaIdVersion) throws SerDesException {
    serDesProtocolHandler.handleSchemaVersionSerialization(os, schemaIdVersion);
  }

  protected void serializePayload(OutputStream os, Object input) throws SerDesException {
    serDesProtocolHandler.handlePayloadSerialization(os, input);
  }

  protected Byte getProtocolId() {
    return serDesProtocolHandler.getProtocolId();
  }

}
