/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.registry.avro;

import org.apache.hadoop.hive.registry.CompatibilityResult;
import org.apache.hadoop.hive.registry.SchemaCompatibility;
import org.apache.hadoop.hive.registry.SchemaValidator;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

/**
 * Evaluate the compatibility between a reader schema and a writer schema.
 * A reader and a writer schema are declared compatible if all datum instances of the writer
 * schema can be successfully decoded using the specified reader schema.
 */
public final class AvroSchemaValidator implements SchemaValidator<Schema> {
    private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaValidator.class);

    private static final Map<SchemaCompatibility, SchemaCompatibilityValidator> COMPATIBILITY_VALIDATORS;

    static {
        AvroSchemaValidator avroSchemaValidator = new AvroSchemaValidator();
        Map<SchemaCompatibility, SchemaCompatibilityValidator<Schema>> validators = new HashMap<>();
        validators.put(SchemaCompatibility.BACKWARD, new BackwardCompatibilityValidator<>(avroSchemaValidator));
        validators.put(SchemaCompatibility.FORWARD, new ForwardCompatibilityValidator<>(avroSchemaValidator));
        validators.put(SchemaCompatibility.BOTH, new BothCompatibilityValidator<>(avroSchemaValidator));
        validators.put(SchemaCompatibility.NONE, new NoneCompatibilityValidator<>());

        COMPATIBILITY_VALIDATORS = Collections.unmodifiableMap(validators);
    }

    static SchemaCompatibilityValidator<Schema> of(SchemaCompatibility compatibility) {
        return COMPATIBILITY_VALIDATORS.get(compatibility);
    }

    /**
     * Utility class cannot be instantiated.
     */
    private AvroSchemaValidator() {
    }

    public CompatibilityResult validate(Schema readerSchema, Schema writerSchema) {
        SchemaPairCompatibility schemaPairCompatibility = checkReaderWriterCompatibility(readerSchema, writerSchema);
        SchemaCompatibilityResult result = schemaPairCompatibility.getResult();
        return result.getCompatibility() == SchemaCompatibilityType.COMPATIBLE
               ? CompatibilityResult.createCompatibleResult(writerSchema.toString())
               : CompatibilityResult.createIncompatibleResult(result.getMessage(),
                                                              result.getLocation(),
                                                              writerSchema.toString());
    }

// -----------------------------------------------------------------------------------------------
// Below code is borrowed from AVRO-2003 and AVRO-1933 patches
// This will be replaced once those patches are merged into apache/avro repo.
// -----------------------------------------------------------------------------------------------

    /**
     * Message to annotate reader/writer schema pairs that are compatible.
     */
    public static final String READER_WRITER_COMPATIBLE_MESSAGE =
            "Reader schema can always successfully decode data written using the writer schema.";

    /**
     * Validates that the provided reader schema can be used to decode avro data
     * written with the provided writer schema.
     *
     * @param reader schema to check.
     * @param writer schema to check.
     *
     * @return a result object identifying any compatibility errors.
     */
    public static SchemaPairCompatibility checkReaderWriterCompatibility(final Schema reader,
                                                                         final Schema writer) {
        final SchemaCompatibilityResult compatibility = new ReaderWriterCompatiblityChecker()
                .getCompatibility(reader, writer);

        final String message;
        switch (compatibility.getCompatibility()) {
            case INCOMPATIBLE: {
                message = String.format("Data encoded using writer schema:%n%s%n"
                                                + "will or may fail to decode using reader schema:%n%s%n",
                                        writer.toString(true),
                                        reader.toString(true));
                break;
            }
            case COMPATIBLE: {
                message = READER_WRITER_COMPATIBLE_MESSAGE;
                break;
            }
            default:
                throw new AvroRuntimeException("Unknown compatibility: " + compatibility);
        }

        return new SchemaPairCompatibility(compatibility,
                                           reader,
                                           writer,
                                           message);
    }

    // -----------------------------------------------------------------------------------------------

    /**
     * Tests the equality of two Avro named schemas.
     * <p>
     * Matching includes reader name aliases.
     * </p>
     *
     * @param reader Named reader schema.
     * @param writer Named writer schema.
     *
     * @return whether the names of the named schemas match or not.
     */
    public static boolean schemaNameEquals(final Schema reader, final Schema writer) {
        final String writerFullName = writer.getFullName();
        if (objectsEqual(reader.getFullName(), writerFullName)) {
            return true;
        }
        // Apply reader aliases:
        if (reader.getAliases().contains(writerFullName)) {
            return true;
        }
        return false;
    }

    /**
     * Identifies the writer field that corresponds to the specified reader field.
     * <p>
     * Matching includes reader name aliases.
     * </p>
     *
     * @param writerSchema Schema of the record where to look for the writer
     *                     field.
     * @param readerField  Reader field to identify the corresponding writer field
     *                     of.
     *
     * @return the writer field, if any does correspond, or None.
     */
    public static Schema.Field lookupWriterField(final Schema writerSchema, final Schema.Field readerField) {
        assert (writerSchema.getType() == Schema.Type.RECORD);
        final List<Schema.Field> writerFields = new ArrayList<>();
        final Schema.Field direct = writerSchema.getField(readerField.name());
        if (direct != null) {
            writerFields.add(direct);
        }
        for (final String readerFieldAliasName : readerField.aliases()) {
            final Schema.Field writerField = writerSchema.getField(readerFieldAliasName);
            if (writerField != null) {
                writerFields.add(writerField);
            }
        }
        switch (writerFields.size()) {
            case 0:
                return null;
            case 1:
                return writerFields.get(0);
            default: {
                throw new AvroRuntimeException(String.format("Reader record field %s matches multiple fields in writer " +
                                                                     "record schema %s",
                                                             readerField,
                                                             writerSchema));
            }
        }
    }

    /**
     * Reader/writer schema pair that can be used as a key in a hash map. This
     * reader/writer pair differentiates Schema objects based on their system hash
     * code.
     */
    private static final class ReaderWriter {
        private final Schema reader;
        private final Schema writer;

        /**
         * Initializes a new reader/writer pair.
         *
         * @param reader Reader schema.
         * @param writer Writer schema.
         */
        public ReaderWriter(final Schema reader, final Schema writer) {
            this.reader = reader;
            this.writer = writer;
        }

        /**
         * Returns the reader schema in this pair.
         *
         * @return the reader schema in this pair.
         */
        public Schema getReader() {
            return reader;
        }

        /**
         * Returns the writer schema in this pair.
         *
         * @return the writer schema in this pair.
         */
        public Schema getWriter() {
            return writer;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int hashCode() {
            return System.identityHashCode(reader) ^ System.identityHashCode(writer);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ReaderWriter)) {
                return false;
            }
            final ReaderWriter that = (ReaderWriter) obj;
            // Use pointer comparison here:
            return (this.reader == that.reader)
                    && (this.writer == that.writer);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return String.format("ReaderWriter{reader:%s, writer:%s}", reader, writer);
        }
    }

    /**
     * Determines the compatibility of a reader/writer schema pair.
     * <p>
     * Provides memoization to handle recursive schemas.
     * </p>
     */
    private static final class ReaderWriterCompatiblityChecker {
        private static final String ROOT_REFERENCE_TOKEN = "";
        private final Map<ReaderWriter, SchemaCompatibilityResult> mMemoizeMap = new HashMap<ReaderWriter, SchemaCompatibilityResult>();

        /**
         * Reports the compatibility of a reader/writer schema pair.
         * <p>
         * Memoizes the compatibility results.
         * </p>
         *
         * @param reader Reader schema to test.
         * @param writer Writer schema to test.
         *
         * @return the compatibility of the reader/writer schema pair.
         */
        public SchemaCompatibilityResult getCompatibility(final Schema reader,
                                                          final Schema writer) {
            Stack<String> location = new Stack<>();
            return getCompatibility(ROOT_REFERENCE_TOKEN, reader, writer, location);
        }

        /**
         * Reports the compatibility of a reader/writer schema pair.
         * <p>
         * Memoizes the compatibility results.
         * </p>
         *
         * @param referenceToken The equivalent JSON pointer reference token representation of the schema node being visited.
         * @param reader         Reader schema to test.
         * @param writer         Writer schema to test.
         * @param location       Stack with which to track the location within the schema.
         *
         * @return the compatibility of the reader/writer schema pair.
         */
        private SchemaCompatibilityResult getCompatibility(String referenceToken,
                                                           final Schema reader,
                                                           final Schema writer,
                                                           final Stack<String> location) {
            location.push(referenceToken);
            LOG.debug("Checking compatibility of reader {} with writer {}", reader, writer);
            final ReaderWriter pair = new ReaderWriter(reader, writer);
            final SchemaCompatibilityResult existing = mMemoizeMap.get(pair);
            if (existing != null) {
                if (existing.getCompatibility() == SchemaCompatibilityType.RECURSION_IN_PROGRESS) {
                    // Break the recursion here.
                    // schemas are compatible unless proven incompatible:
                    location.pop();
                    return SchemaCompatibilityResult.compatible();
                }
                return existing;
            }
            // Mark this reader/writer pair as "in progress":
            mMemoizeMap.put(pair, SchemaCompatibilityResult.recursionInProgress());
            final SchemaCompatibilityResult calculated = calculateCompatibility(reader, writer, location);
            if (calculated == SchemaCompatibilityResult.COMPATIBLE) {
                location.pop();
            }
            mMemoizeMap.put(pair, calculated);
            return calculated;
        }

        /**
         * Calculates the compatibility of a reader/writer schema pair.
         * <p>
         * Relies on external memoization performed by
         * {@link #getCompatibility(Schema, Schema)}.
         * </p>
         *
         * @param reader Reader schema to test.
         * @param writer Writer schema to test.
         *
         * @return the compatibility of the reader/writer schema pair.
         */
        private SchemaCompatibilityResult calculateCompatibility(final Schema reader,
                                                                 final Schema writer,
                                                                 final Stack<String> location) {
            assert (reader != null);
            assert (writer != null);

            if (reader.getType() == writer.getType()) {
                switch (reader.getType()) {
                    case NULL:
                    case BOOLEAN:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case BYTES:
                    case STRING: {
                        return SchemaCompatibilityResult.compatible();
                    }
                    case ARRAY: {
                        return getCompatibility("items", reader.getElementType(), writer.getElementType(), location);
                    }
                    case MAP: {
                        return getCompatibility("values", reader.getValueType(), writer.getValueType(), location);
                    }
                    case FIXED: {
                        SchemaCompatibilityResult nameCheck = checkSchemaNames(reader, writer, location);
                        if (nameCheck.getCompatibility() == SchemaCompatibilityType.INCOMPATIBLE) {
                            return nameCheck;
                        }
                        return checkFixedSize(reader, writer, location);
                    }
                    case ENUM: {
                        SchemaCompatibilityResult nameCheck = checkSchemaNames(reader, writer, location);
                        if (nameCheck.getCompatibility() == SchemaCompatibilityType.INCOMPATIBLE) {
                            return nameCheck;
                        }
                        return checkReaderEnumContainsAllWriterEnumSymbols(reader, writer, location);
                    }
                    case RECORD: {
                        SchemaCompatibilityResult nameCheck = checkSchemaNames(reader, writer, location);
                        if (nameCheck.getCompatibility() == SchemaCompatibilityType.INCOMPATIBLE) {
                            return nameCheck;
                        }
                        return checkReaderWriterRecordFields(reader, writer, location);
                    }
                    case UNION: {
                        // Check that each individual branch of the writer union can be
                        // decoded:
                        int i = 0;
                        for (final Schema writerBranch : writer.getTypes()) {
                            location.push(Integer.toString(i));
                            SchemaCompatibilityResult compatibility = getCompatibility(reader, writerBranch);
                            if (compatibility.getCompatibility() == SchemaCompatibilityType.INCOMPATIBLE) {
                                String message = String.format("reader union lacking writer type: %s",
                                                               writerBranch.getType());
                                return SchemaCompatibilityResult.incompatible(
                                        SchemaIncompatibilityType.MISSING_UNION_BRANCH,
                                        reader, writer, message, location);
                            }
                            location.pop();
                            i++;
                        }
                        // Each schema in the writer union can be decoded with the reader:
                        return SchemaCompatibilityResult.compatible();
                    }

                    default: {
                        throw new AvroRuntimeException("Unknown schema type: " + reader.getType());
                    }
                }

            } else {
                // Reader and writer have different schema types:

                // Reader compatible with all branches of a writer union is compatible
                if (writer.getType() == Schema.Type.UNION) {
                    int i = 0;
                    for (Schema s : writer.getTypes()) {
                        location.push(Integer.toString(i));
                        SchemaCompatibilityResult result = getCompatibility(reader, s);
                        if (result.getCompatibility() == SchemaCompatibilityType.INCOMPATIBLE) {
                            return result;
                        }
                        location.pop();
                    }
                    return SchemaCompatibilityResult.compatible();
                }

                switch (reader.getType()) {
                    case NULL:
                        return typeMismatch(reader, writer, location);
                    case BOOLEAN:
                        return typeMismatch(reader, writer, location);
                    case INT:
                        return typeMismatch(reader, writer, location);
                    case LONG: {
                        return (writer.getType() == Schema.Type.INT)
                               ? SchemaCompatibilityResult.compatible()
                               : typeMismatch(reader, writer, location);
                    }
                    case FLOAT: {
                        return ((writer.getType() == Schema.Type.INT)
                                || (writer.getType() == Schema.Type.LONG))
                               ? SchemaCompatibilityResult.compatible()
                               : typeMismatch(reader, writer, location);

                    }
                    case DOUBLE: {
                        return ((writer.getType() == Schema.Type.INT)
                                || (writer.getType() == Schema.Type.LONG)
                                || (writer.getType() == Schema.Type.FLOAT))
                               ? SchemaCompatibilityResult.compatible()
                               : typeMismatch(reader, writer, location);
                    }
                    case BYTES: {
                        return (writer.getType() == Schema.Type.STRING)
                               ? SchemaCompatibilityResult.compatible()
                               : typeMismatch(reader, writer, location);
                    }
                    case STRING: {
                        return (writer.getType() == Schema.Type.BYTES)
                               ? SchemaCompatibilityResult.compatible()
                               : typeMismatch(reader, writer, location);
                    }

                    case ARRAY:
                        return typeMismatch(reader, writer, location);
                    case MAP:
                        return typeMismatch(reader, writer, location);
                    case FIXED:
                        return typeMismatch(reader, writer, location);
                    case ENUM:
                        return typeMismatch(reader, writer, location);
                    case RECORD:
                        return typeMismatch(reader, writer, location);
                    case UNION: {
                        for (final Schema readerBranch : reader.getTypes()) {
                            SchemaCompatibilityResult compatibility = getCompatibility(readerBranch, writer);
                            if (compatibility.getCompatibility() == SchemaCompatibilityType.COMPATIBLE) {
                                return SchemaCompatibilityResult.compatible();
                            }
                        }
                        // No branch in the reader union has been found compatible with the
                        // writer schema:
                        String message = String.format("reader union lacking writer type: %s", writer.getType());
                        return SchemaCompatibilityResult.incompatible(
                                SchemaIncompatibilityType.MISSING_UNION_BRANCH,
                                reader, writer, message, location);
                    }

                    default: {
                        throw new AvroRuntimeException("Unknown schema type: " + reader.getType());
                    }
                }
            }
        }

        private SchemaCompatibilityResult checkReaderWriterRecordFields(final Schema reader,
                                                                        final Schema writer,
                                                                        final Stack<String> location) {
            location.push("fields");
            // Check that each field in the reader record can be populated from
            // the writer record:
            for (final Schema.Field readerField : reader.getFields()) {
                location.push(Integer.toString(readerField.pos()));
                final Schema.Field writerField = lookupWriterField(writer, readerField);
                Schema readerFieldSchema = readerField.schema();
                if (writerField == null) {
                    // Reader field does not correspond to any field in the writer
                    // record schema, so the reader field must have a default value.
                    if (readerField.defaultValue() == null) {
                        if (!isUnionWithFirstTypeAsNull(readerFieldSchema)) {
                            // reader field has no default value
                            String message = String.format("Reader schema missing default value for field: %s", readerField
                                    .name());
                            return SchemaCompatibilityResult.incompatible(
                                    SchemaIncompatibilityType.READER_FIELD_MISSING_DEFAULT_VALUE, reader, writer,
                                    message, location);
                        }
                    }
                } else {
                    SchemaCompatibilityResult compatibility = getCompatibility("type", readerFieldSchema,
                                                                               writerField.schema(), location);
                    if (compatibility.getCompatibility() == SchemaCompatibilityType.INCOMPATIBLE) {
                        return compatibility;
                    }
                }
                location.pop();
            }
            // All fields in the reader record can be populated from the writer
            // record:
            location.pop();
            return SchemaCompatibilityResult.compatible();
        }

        private boolean isUnionWithFirstTypeAsNull(Schema readerFieldSchema) {
            return readerFieldSchema.getType() == Schema.Type.UNION
                    && readerFieldSchema.getTypes().get(0).getType() == Schema.Type.NULL;
        }

        private SchemaCompatibilityResult checkReaderEnumContainsAllWriterEnumSymbols(
                final Schema reader, final Schema writer, final Stack<String> location) {
            location.push("symbols");
            final Set<String> symbols = new TreeSet<String>(writer.getEnumSymbols());
            symbols.removeAll(reader.getEnumSymbols());
            if (!symbols.isEmpty()) {
                return SchemaCompatibilityResult.incompatible(
                        SchemaIncompatibilityType.MISSING_ENUM_SYMBOLS, reader, writer,
                        symbols.toString(), location);
            }
            location.pop();
            return SchemaCompatibilityResult.compatible();
        }

        private SchemaCompatibilityResult checkFixedSize(final Schema reader,
                                                         final Schema writer,
                                                         final Stack<String> location) {
            location.push("size");
            int actual = reader.getFixedSize();
            int expected = writer.getFixedSize();
            if (actual != expected) {
                String message = String.format("expected: %d, found: %d", expected, actual);
                return SchemaCompatibilityResult.incompatible(
                        SchemaIncompatibilityType.FIXED_SIZE_MISMATCH, reader,
                        writer, message, location);
            }
            location.pop();
            return SchemaCompatibilityResult.compatible();
        }

        private SchemaCompatibilityResult checkSchemaNames(final Schema reader,
                                                           final Schema writer,
                                                           final Stack<String> location) {
            location.push("name");
            if (!schemaNameEquals(reader, writer)) {
                String message = String.format("expected: %s", writer.getFullName());
                return SchemaCompatibilityResult.incompatible(
                        SchemaIncompatibilityType.NAME_MISMATCH,
                        reader, writer, message, location);
            }
            location.pop();
            return SchemaCompatibilityResult.compatible();
        }

        private SchemaCompatibilityResult typeMismatch(final Schema reader,
                                                       final Schema writer,
                                                       final Stack<String> location) {
            String message = String.format("reader type: %s not compatible with writer type: %s",
                                           reader.getType(), writer.getType());
            return SchemaCompatibilityResult.incompatible(SchemaIncompatibilityType.TYPE_MISMATCH,
                                                          reader, writer, message, location);
        }
    }

    /**
     * Identifies the type of a schema compatibility result.
     */
    public enum SchemaCompatibilityType {
        COMPATIBLE, //
        INCOMPATIBLE, //

        /**
         * Used internally to tag a reader/writer schema pair and prevent recursion.
         */
        RECURSION_IN_PROGRESS;
    }

    public enum SchemaIncompatibilityType {
        NAME_MISMATCH, //
        FIXED_SIZE_MISMATCH, //
        MISSING_ENUM_SYMBOLS, //
        READER_FIELD_MISSING_DEFAULT_VALUE, //
        TYPE_MISMATCH, //
        MISSING_UNION_BRANCH;
    }

    /**
     * Immutable class representing details about a particular schema pair
     * compatibility check.
     */
    public static final class SchemaCompatibilityResult {
        private final SchemaCompatibilityType compatibility;
        // the below fields are only valid if INCOMPATIBLE
        private final SchemaIncompatibilityType schemaIncompatibilityType;
        private final Schema readerSubset;
        private final Schema writerSubset;
        private final String message;
        private final List<String> location;
        // cached objects for stateless details
        private static final SchemaCompatibilityResult COMPATIBLE = new SchemaCompatibilityResult(
                SchemaCompatibilityType.COMPATIBLE, null, null, null, null, null);
        private static final SchemaCompatibilityResult RECURSION_IN_PROGRESS = new SchemaCompatibilityResult(
                SchemaCompatibilityType.RECURSION_IN_PROGRESS, null, null, null, null, null);

        private SchemaCompatibilityResult(SchemaCompatibilityType type,
                                          SchemaIncompatibilityType errorDetails,
                                          Schema readerDetails,
                                          Schema writerDetails,
                                          String message,
                                          List<String> location) {
            this.compatibility = type;
            this.schemaIncompatibilityType = errorDetails;
            this.readerSubset = readerDetails;
            this.writerSubset = writerDetails;
            this.message = message;
            this.location = location;
        }

        /**
         * Returns a details object representing a compatible schema pair.
         *
         * @return a SchemaCompatibilityResult object with COMPATIBLE
         * SchemaCompatibilityType, and no other state.
         */
        public static SchemaCompatibilityResult compatible() {
            return COMPATIBLE;
        }

        /**
         * Returns a details object representing a state indicating that recursion
         * is in progress.
         *
         * @return a SchemaCompatibilityResult object with RECURSION_IN_PROGRESS
         * SchemaCompatibilityType, and no other state.
         */
        private static SchemaCompatibilityResult recursionInProgress() {
            return RECURSION_IN_PROGRESS;
        }

        /**
         * Returns a details object representing an incompatible schema pair,
         * including error details.
         *
         * @param error type of error
         * @param reader reader schema
         * @param writer writer schema
         * @param message message
         * @param location location of error
         *
         * @return a SchemaCompatibilityResult object with INCOMPATIBLE
         * SchemaCompatibilityType, and state representing the violating
         * part.
         */
        public static SchemaCompatibilityResult incompatible(SchemaIncompatibilityType error,
                                                             Schema reader,
                                                             Schema writer,
                                                             String message,
                                                             List<String> location) {
            return new SchemaCompatibilityResult(SchemaCompatibilityType.INCOMPATIBLE, error, reader,
                                                 writer, message, location);
        }

        /**
         * Returns the SchemaCompatibilityType, always non-null.
         *
         * @return a SchemaCompatibilityType instance, always non-null
         */
        public SchemaCompatibilityType getCompatibility() {
            return compatibility;
        }

        /**
         * If the compatibility is INCOMPATIBLE, returns the
         * SchemaIncompatibilityType (first thing that was incompatible), otherwise
         * null.
         *
         * @return a SchemaIncompatibilityType instance, or null
         */
        public SchemaIncompatibilityType getIncompatibility() {
            return schemaIncompatibilityType;
        }

        /**
         * If the compatibility is INCOMPATIBLE, returns the first part of the
         * reader schema that failed compatibility check.
         *
         * @return a Schema instance (part of the reader schema), or null
         */
        public Schema getReaderSubset() {
            return readerSubset;
        }

        /**
         * If the compatibility is INCOMPATIBLE, returns the first part of the
         * writer schema that failed compatibility check.
         *
         * @return a Schema instance (part of the writer schema), or null
         */
        public Schema getWriterSubset() {
            return writerSubset;
        }

        /**
         * If the compatibility is INCOMPATIBLE, returns a human-readable message
         * with more details about what failed. Syntax depends on the
         * SchemaIncompatibilityType.
         *
         * @return a String with details about the incompatibility, or null
         *
         * @see #getIncompatibility()
         */
        public String getMessage() {
            return message;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((message == null) ? 0 : message.hashCode());
            result = prime * result + ((readerSubset == null) ? 0 : readerSubset.hashCode());
            result = prime * result
                    + ((compatibility == null) ? 0 : compatibility.hashCode());
            result = prime * result
                    + ((schemaIncompatibilityType == null) ? 0 : schemaIncompatibilityType.hashCode());
            result = prime * result + ((writerSubset == null) ? 0 : writerSubset.hashCode());
            result = prime * result + ((location == null) ? 0 : location.hashCode());
            return result;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            SchemaCompatibilityResult other = (SchemaCompatibilityResult) obj;
            if (message == null) {
                if (other.message != null)
                    return false;
            } else if (!message.equals(other.message))
                return false;
            if (readerSubset == null) {
                if (other.readerSubset != null)
                    return false;
            } else if (!readerSubset.equals(other.readerSubset))
                return false;
            if (compatibility != other.compatibility)
                return false;
            if (schemaIncompatibilityType != other.schemaIncompatibilityType)
                return false;
            if (writerSubset == null) {
                if (other.writerSubset != null)
                    return false;
            } else if (!writerSubset.equals(other.writerSubset))
                return false;
            if (location == null) {
                if (other.location != null)
                    return false;
            } else if (!location.equals(other.location))
                return false;
            return true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return String.format(
                    "SchemaCompatibilityResult{compatibility:%s, type:%s, readerSubset:%s, writerSubset:%s, message:%s, location:%s}",
                    compatibility, schemaIncompatibilityType, readerSubset, writerSubset, message, getLocation());
        }

        /**
         * Returns a <a href="https://tools.ietf.org/html/draft-ietf-appsawg-json-pointer-08">JSON Pointer</a> describing
         * the node location within the schema's JSON document tree where the incompatibility was encountered.
         *
         * @return JSON Pointer encoded as a string or {@code null} if there was no incompatibility.
         */
        public String getLocation() {
            if (compatibility != SchemaCompatibilityType.INCOMPATIBLE) {
                return null;
            }
            StringBuilder s = new StringBuilder("/");
            boolean first = true;
            // ignore root element
            for (String coordinate : location.subList(1, location.size())) {
                if (first) {
                    first = false;
                } else {
                    s.append('/');
                }
                // Apply JSON pointer escaping.
                s.append(coordinate.replace("~", "~0").replace("/", "~1"));
            }
            return s.toString();
        }
    }
    // -----------------------------------------------------------------------------------------------

    /**
     * Provides information about the compatibility of a single reader and writer
     * schema pair. Note: This class represents a one-way relationship from the
     * reader to the writer schema.
     */
    public static final class SchemaPairCompatibility {
        /**
         * The details of this result.
         */
        private final SchemaCompatibilityResult result;

        /**
         * Validated reader schema.
         */
        private final Schema reader;

        /**
         * Validated writer schema.
         */
        private final Schema writer;

        /**
         * Human readable description of this result.
         */
        private final String description;

        /**
         * Constructs a new instance.
         *
         * @param result      The result of the compatibility check.
         * @param reader      schema that was validated.
         * @param writer      schema that was validated.
         * @param description of this compatibility result.
         */
        public SchemaPairCompatibility(SchemaCompatibilityResult result,
                                       Schema reader,
                                       Schema writer,
                                       String description) {
            this.result = result;
            this.reader = reader;
            this.writer = writer;
            this.description = description;
        }

        /**
         * Gets the type of this result.
         *
         * @return the type of this result.
         */
        public SchemaCompatibilityType getType() {
            return result.getCompatibility();
        }

        /**
         * Gets more details about the compatibility, in particular if getType() is INCOMPATIBLE.
         *
         * @return the details of this compatibility check.
         */
        public SchemaCompatibilityResult getResult() {
            return result;
        }

        /**
         * Gets the reader schema that was validated.
         *
         * @return reader schema that was validated.
         */
        public Schema getReader() {
            return reader;
        }

        /**
         * Gets the writer schema that was validated.
         *
         * @return writer schema that was validated.
         */
        public Schema getWriter() {
            return writer;
        }

        /**
         * Gets a human readable description of this validation result.
         *
         * @return a human readable description of this validation result.
         */
        public String getDescription() {
            return description;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return String.format(
                    "SchemaPairCompatibility{result:%s, readerSchema:%s, writerSchema:%s, description:%s}",
                    result, reader, writer, description);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean equals(Object other) {
            if ((null != other) && (other instanceof SchemaPairCompatibility)) {
                final SchemaPairCompatibility result = (SchemaPairCompatibility) other;
                return objectsEqual(result.result, this.result)
                        && objectsEqual(result.reader, reader)
                        && objectsEqual(result.writer, writer)
                        && objectsEqual(result.description, description);
            } else {
                return false;
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int hashCode() {
            return Arrays.hashCode(new Object[]{result, reader, writer, description});
        }
    }

    /**
     * Borrowed from Guava's Objects.equal(a, b)
     */
    private static boolean objectsEqual(Object obj1, Object obj2) {
        return (obj1 == obj2) || ((obj1 != null) && obj1.equals(obj2));
    }

}
