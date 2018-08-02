package org.apache.hadoop.hive.registry.serdes;

import org.apache.hadoop.hive.registry.SchemaIdVersion;
import org.apache.hadoop.hive.registry.SchemaMetadata;
import org.apache.hadoop.hive.registry.SchemaVersion;
import org.apache.hadoop.hive.registry.client.ISchemaRegistryClient;
import org.apache.hadoop.hive.registry.common.errors.IncompatibleSchemaException;
import org.apache.hadoop.hive.registry.common.errors.InvalidSchemaException;
import org.apache.hadoop.hive.registry.common.errors.SchemaBranchNotFoundException;
import org.apache.hadoop.hive.registry.common.errors.SchemaNotFoundException;
import org.apache.hadoop.hive.registry.common.exception.RegistryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements {@link SnapshotSerializer} and internally creates schema registry client to connect to the
 * target schema registry.
 *
 * Extensions of this class need to implement below methods.
 * <ul>
 *    <li>{@link #doSerialize(Object, SchemaIdVersion)}</li>
 *    <li>{@link #getSchemaText(Object)}</li>
 * </ul>
 */
public abstract class AbstractSnapshotSerializer<I, O> extends AbstractSerDes implements SnapshotSerializer<I, O, SchemaMetadata> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractSnapshotSerializer.class);

  public AbstractSnapshotSerializer() {
  }

  public AbstractSnapshotSerializer(ISchemaRegistryClient schemaRegistryClient) {
    super(schemaRegistryClient);
  }

  @Override
  public final O serialize(I input, SchemaMetadata schemaMetadata) throws SerDesException {
    if(!initialized) {
      throw new IllegalStateException("init should be invoked before invoking serialize operation");
    }
    if(closed) {
      throw new IllegalStateException("This serializer is already closed");
    }

    // compute schema based on input object
    String schema = getSchemaText(input);

    // register that schema and get the version
    try {
      SchemaIdVersion schemaIdVersion = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema, "Schema registered by serializer:" + this.getClass()));
      // write the version and given object to the output
      return doSerialize(input, schemaIdVersion);
    } catch (SchemaNotFoundException | IncompatibleSchemaException | InvalidSchemaException | SchemaBranchNotFoundException e) {
      throw new RegistryException(e);
    }
  }

  /**
   * Returns textual representation of the schema for the given {@code input} payload.
   * @param input input payload
   */
  protected abstract String getSchemaText(I input);

  /**
   * Returns the serialized object (which can be byte array or inputstream or any other object) which may contain all
   * the required information for deserializer to deserialize into the given {@code input}.
   *
   * @param input input object to be serialized
   * @param schemaIdVersion schema version info of the given input
   * @throws SerDesException when any ser/des Exception occurs
   */
  protected abstract O doSerialize(I input, SchemaIdVersion schemaIdVersion) throws SerDesException;

}
