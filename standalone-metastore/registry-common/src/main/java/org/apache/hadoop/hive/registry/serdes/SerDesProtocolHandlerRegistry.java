package org.apache.hadoop.hive.registry.serdes;

import org.apache.hadoop.hive.registry.serdes.avro.ConfluentProtocolHandler;
import org.apache.hadoop.hive.registry.serdes.avro.SchemaMetadataIdProtocolHandler;
import org.apache.hadoop.hive.registry.serdes.avro.SchemaVersionIdAsIntProtocolHandler;
import org.apache.hadoop.hive.registry.serdes.avro.SchemaVersionIdAsLongProtocolHandler;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registered ser/des protocol handlers which can be used in serializers/deserializers.
 */
public final class SerDesProtocolHandlerRegistry {

  public static final byte CONFLUENT_VERSION_PROTOCOL = 0x0;
  public static final byte METADATA_ID_VERSION_PROTOCOL = 0x1;
  public static final byte VERSION_ID_AS_LONG_PROTOCOL = 0x2;
  public static final byte VERSION_ID_AS_INT_PROTOCOL = 0x3;
  public static final byte CURRENT_PROTOCOL = VERSION_ID_AS_INT_PROTOCOL;

  private static final SerDesProtocolHandlerRegistry instance = new SerDesProtocolHandlerRegistry();

  private final Map<Byte, SerDesProtocolHandler> protocolWithHandlers = new ConcurrentHashMap<>();

  public static SerDesProtocolHandlerRegistry get() {
    return instance;
  }

  public SerDesProtocolHandler getSerDesProtocolHandler(Byte protocolId) {
    return protocolWithHandlers.get(protocolId);
  }

  public Map<Byte, SerDesProtocolHandler> getRegisteredSerDesProtocolHandlers() {
    return Collections.unmodifiableMap(protocolWithHandlers);
  }

  private SerDesProtocolHandlerRegistry() {
    List<SerDesProtocolHandler> inbuiltHandlers = Arrays.asList(new ConfluentProtocolHandler(), new SchemaMetadataIdProtocolHandler(),
                                                                new SchemaVersionIdAsIntProtocolHandler(), new SchemaVersionIdAsLongProtocolHandler());
    for (SerDesProtocolHandler inbuiltHandler : inbuiltHandlers) {
      registerSerDesProtocolHandler(inbuiltHandler);
    }
  }

  /**
   * Registers the given protocol handler with it's protocol id.
   *
   * @param serDesProtocolHandler handler to be registered.
   */
  public void registerSerDesProtocolHandler(SerDesProtocolHandler serDesProtocolHandler) {
    SerDesProtocolHandler existingHandler = protocolWithHandlers.putIfAbsent(serDesProtocolHandler.getProtocolId(), serDesProtocolHandler);
    if (existingHandler != null) {
      throw new IllegalArgumentException("SerDesProtocolHandler is already registered with the given protocol id: " + serDesProtocolHandler.getProtocolId());
    }
  }

}
