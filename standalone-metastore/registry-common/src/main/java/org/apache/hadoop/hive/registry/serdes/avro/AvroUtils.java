package org.apache.hadoop.hive.registry.serdes.avro;

import org.apache.hadoop.hive.registry.serdes.avro.exceptions.AvroException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Utils class for Avro related functionality.
 */
public final class AvroUtils {
  public static final Charset UTF_8 = Charset.forName("UTF-8");

  private static final Map<Schema.Type, Schema> PRIMITIVE_SCHEMAS;

  static {
    Map<Schema.Type, Schema> map = new HashMap<>();
    Schema.Type[] types = {Schema.Type.NULL, Schema.Type.BYTES, Schema.Type.INT, Schema.Type.FLOAT,
            Schema.Type.DOUBLE, Schema.Type.LONG, Schema.Type.STRING, Schema.Type.BOOLEAN};
    for (Schema.Type type : types) {
      map.put(type, Schema.create(type));
    }
    PRIMITIVE_SCHEMAS = Collections.unmodifiableMap(map);
  }

  private AvroUtils() {
  }

  public static Schema getSchemaForPrimitives(Object input) {
    Schema.Type type;
    if (input == null) {
      type = Schema.Type.NULL;
    } else if (input instanceof byte[]) {
      type = Schema.Type.BYTES;
    } else if (input instanceof Integer || input instanceof Short || input instanceof Byte) {
      type = Schema.Type.INT;
    } else if (input instanceof Float) {
      type = Schema.Type.FLOAT;
    } else if (input instanceof Double) {
      type = Schema.Type.DOUBLE;
    } else if (input instanceof Long) {
      type = Schema.Type.LONG;
    } else if (input instanceof String) {
      type = Schema.Type.STRING;
    } else if (input instanceof Boolean) {
      type = Schema.Type.BOOLEAN;
    } else {
      throw new AvroException("input type: " + input.getClass() + " is not supported");
    }

    return PRIMITIVE_SCHEMAS.get(type);

  }

  public static Schema computeSchema(Object input) {
    Schema schema = null;
    if (input instanceof GenericContainer) {
      schema = ((GenericContainer) input).getSchema();
    } else {
      schema = AvroUtils.getSchemaForPrimitives(input);
    }
    return schema;
  }

}
