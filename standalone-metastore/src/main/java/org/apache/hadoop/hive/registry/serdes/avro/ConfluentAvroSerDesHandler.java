package org.apache.hadoop.hive.registry.serdes.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.registry.serdes.avro.exceptions.AvroException;
import org.apache.hadoop.hive.registry.serdes.avro.exceptions.AvroRetryableException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConfluentAvroSerDesHandler implements AvroSerDesHandler {
  private final Map<String, Schema> readerSchemaCache = new ConcurrentHashMap<>();

  @Override
  public void handlePayloadSerialization(OutputStream outputStream, Object input) {
    try {
      Schema schema = AvroUtils.computeSchema(input);
      if (input instanceof byte[]) {
        outputStream.write((byte[]) input);
      } else {
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        DatumWriter<Object> writer;
        if (input instanceof SpecificRecord) {
          writer = new SpecificDatumWriter<>(schema);
        } else {
          writer = new GenericDatumWriter<>(schema);
        }
        writer.write(input, encoder);
        encoder.flush();
      }
    } catch (IOException e) {
      throw new AvroRetryableException("Error serializing Avro message", e);
    } catch (RuntimeException e) {
      // avro serialization can throw AvroRuntimeException, NullPointerException,
      // ClassCastException, etc
      throw new AvroException("Error serializing Avro message", e);
    }
  }

  @Override
  public Object handlePayloadDeserialization(InputStream payloadInputStream,
                                             Schema writerSchema,
                                             Schema readerSchema,
                                             boolean useSpecificAvroReader) {

    Object deserializedObj;
    try {
      if (Schema.Type.BYTES.equals(writerSchema.getType())) {
        // serializer writes byte array directly without going through avro encoder layers.
        deserializedObj = IOUtils.toByteArray(payloadInputStream);
      } else {
        DatumReader datumReader = getDatumReader(writerSchema, readerSchema, useSpecificAvroReader);
        deserializedObj = datumReader.read(null, DecoderFactory.get().binaryDecoder(payloadInputStream, null));
      }
    } catch (IOException e) {
      throw new AvroRetryableException("Error deserializing Avro message for id " + writerSchema, e);
    } catch (RuntimeException e) {
      // avro deserialization may throw AvroRuntimeException, NullPointerException, etc
      throw new AvroException("Error deserializing Avro message for id " + writerSchema, e);
    }
    return deserializedObj;
  }

  private DatumReader getDatumReader(Schema writerSchema, Schema readerSchema, boolean useSpecificAvroReader) {
    if (useSpecificAvroReader) {
      if (readerSchema == null) {
        readerSchema = this.getReaderSchema(writerSchema);
      }

      return new SpecificDatumReader(writerSchema, readerSchema);
    } else {
      return readerSchema == null ? new GenericDatumReader(writerSchema) : new GenericDatumReader(writerSchema, readerSchema);
    }
  }

  private Schema getReaderSchema(Schema writerSchema) {
    Schema readerSchema = this.readerSchemaCache.get(writerSchema.getFullName());
    if (readerSchema == null) {
      Class readerClass = SpecificData.get().getClass(writerSchema);
      if (readerClass == null) {
        throw new AvroException("Could not find class " + writerSchema.getFullName() + " specified in writer\'s schema whilst finding reader\'s schema for a SpecificRecord.");
      }
      try {
        readerSchema = ((SpecificRecord) readerClass.newInstance()).getSchema();
      } catch (InstantiationException e) {
        throw new AvroException(writerSchema.getFullName() + " specified by the " + "writers schema could not be instantiated to find the readers schema.");
      } catch (IllegalAccessException e) {
        throw new AvroException(writerSchema.getFullName() + " specified by the " + "writers schema is not allowed to be instantiated to find the readers schema.");
      }

      this.readerSchemaCache.put(writerSchema.getFullName(), readerSchema);
    }

    return readerSchema;
  }
}
