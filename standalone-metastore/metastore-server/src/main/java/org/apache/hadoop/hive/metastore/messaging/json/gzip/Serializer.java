package org.apache.hadoop.hive.metastore.messaging.json.gzip;

import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.GZIPOutputStream;

class Serializer implements MessageSerializer {
  private static final Logger LOG = LoggerFactory.getLogger(Serializer.class.getName());

  @Override
  public String serialize(EventMessage message) {
    String messageAsString = MessageSerializer.super.serialize(message);
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      GZIPOutputStream gout = new GZIPOutputStream(baos);
      gout.write(messageAsString.getBytes(StandardCharsets.UTF_8));
      gout.close();
      byte[] compressed = baos.toByteArray();
      return new String(Base64.getEncoder().encode(compressed), StandardCharsets.UTF_8);
    } catch (IOException e) {
      LOG.error("could not use gzip output stream", e);
      LOG.debug("message " + messageAsString);
      throw new RuntimeException("could not use the gzip output Stream", e);
    }
  }
}
