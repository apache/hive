package org.apache.hive.hcatalog.api;

import org.apache.hive.hcatalog.common.HCatException;

/**
 * Interface to serialize HCat API elements.
 */
abstract class MetadataSerializer {

  // Prevent construction outside the get() method.
  protected MetadataSerializer() {}

  /**
   * Static getter method for the appropriate MetadataSerializer implementation.
   * @return MetadataSerializer sub-class.
   * @throws HCatException On failure to construct a concrete MetadataSerializer.
   */
  public static MetadataSerializer get() throws HCatException {
    return new MetadataJSONSerializer();
  }

  /**
   * Serializer for HCatTable instances.
   * @param hcatTable The HCatTable operand, to be serialized.
   * @return Serialized (i.e. String-ified) HCatTable.
   * @throws HCatException On failure to serialize.
   */
  public abstract String serializeTable(HCatTable hcatTable) throws HCatException ;

  /**
   * Deserializer for HCatTable string-representations.
   * @param hcatTableStringRep Serialized HCatTable String (gotten from serializeTable()).
   * @return Deserialized HCatTable instance.
   * @throws HCatException On failure to deserialize (e.g. incompatible serialization format, etc.)
   */
  public abstract HCatTable deserializeTable(String hcatTableStringRep) throws HCatException;

  /**
   * Serializer for HCatPartition instances.
   * @param hcatPartition The HCatPartition operand, to be serialized.
   * @return Serialized (i.e. String-ified) HCatPartition.
   * @throws HCatException On failure to serialize.
   */
  public abstract String serializePartition(HCatPartition hcatPartition) throws HCatException;

  /**
   * Deserializer for HCatPartition string-representations.
   * @param hcatPartitionStringRep Serialized HCatPartition String (gotten from serializePartition()).
   * @return Deserialized HCatPartition instance.
   * @throws HCatException On failure to deserialize (e.g. incompatible serialization format, etc.)
   */
  public abstract HCatPartition deserializePartition(String hcatPartitionStringRep) throws HCatException;

}
