package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.List;

/**
 * This interface must be implemented and used to load the various primitive types
 * in the PrimitiveTypeEntry tables so that they can be used to fetching the mapping
 * between the classname, string representations, Java class equivalents of the Types
 * while parsing using TypeInfoParser
 */
@InterfaceAudience.LimitedPrivate({"standalone-metastore", "hive"})
@InterfaceStability.Evolving
public interface TypeRegistry {
  /**
   * Returns a list of supported PrimitiveTypeEntry objects which can be loaded
   * in the PrimitiveTypeEntry registry to map string values to classnames implementing
   * the types
   * @return List of supported PrimitiveTypeEntry objects which can used for registering
   * the supported primitive types
   */
  List<PrimitiveTypeEntry> getPrimitiveTypeEntries();
}