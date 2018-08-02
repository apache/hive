package org.apache.hadoop.hive.registry.serdes.avro;

import org.apache.hadoop.hive.registry.SchemaIdVersion;
import org.apache.hadoop.hive.registry.client.ISchemaRegistryClient;
import org.apache.hadoop.hive.registry.serdes.SerDesException;
import org.apache.hadoop.hive.registry.serdes.avro.exceptions.AvroRetryableException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * This is the default implementation of {@link AbstractAvroSnapshotDeserializer}.
 * <p>
 * <p>Common way to use this serializer implementation is like below as mentioned in {@link org.apache.hadoop.hive.registry.serdes.SnapshotSerializer}. </p>
 * <pre>{@code
 *     AvroSnapshotSerializer serializer = new AvroSnapshotSerializer();
 *     // initialize with given configuration
 *     serializer.init(config);
 *
 *     // this instance can be used for multiple serialization invocations
 *     serializer.serialize(input, schema);
 *
 *     // close it to release any resources held
 *     serializer.close();
 * }</pre>
 */
public class AvroSnapshotSerializer extends AbstractAvroSnapshotSerializer<byte[]> {

  public AvroSnapshotSerializer() {
  }

  public AvroSnapshotSerializer(ISchemaRegistryClient schemaRegistryClient) {
    super(schemaRegistryClient);
  }

  protected byte[] doSerialize(Object input, SchemaIdVersion schemaIdVersion) throws SerDesException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      serializeSchemaVersion(baos, schemaIdVersion);
      serializePayload(baos, input);

      return baos.toByteArray();
    } catch (IOException e) {
      throw new AvroRetryableException(e);
    }
  }

}

