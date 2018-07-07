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

package org.apache.hadoop.hive.registry.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedHashMultiset;
import com.google.common.collect.Multiset;
import org.apache.hadoop.hive.registry.common.exception.ParserException;

import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

//TODO Make this class Jackson Compatible.
public class Schema implements Serializable {
    public enum Type {
        // Don't change the order of this enum to prevent bugs. If you need to add a new entry do so by adding it to the end.
        BOOLEAN(Boolean.class),
        BYTE(Byte.class), // 8-bit signed integer
        SHORT(Short.class), // 16-bit
        INTEGER(Integer.class), // 32-bit
        LONG(Long.class), // 64-bit
        FLOAT(Float.class),
        DOUBLE(Double.class),
        STRING(String.class),
        BINARY(byte[].class), // raw data
        NESTED(Map.class),  // nested field
        ARRAY(List.class),  // array field
        BLOB(InputStream.class);    // Blob

        private final Class<?> javaType;

        Type(Class<?> javaType) {
            this.javaType = javaType;
        }

        public Class<?> getJavaType() {
            return javaType;
        }

        public boolean valueOfSameType(Object value) throws ParserException {
            return value == null || this.equals(Schema.fromJavaType(value));
        }

        /**
         * Determines the {@link Type} of the value specified
         * @param val value for which to determine the type
         * @return {@link Type} of the value
         */
        public static Type getTypeOfVal(String val) {
            Type type = null;
            Type[] types = Type.values();

            if (val != null && (val.equalsIgnoreCase("true") || val.equalsIgnoreCase("false"))) {
                type = BOOLEAN;
            }

            for (int i = 1; type == null && i < STRING.ordinal(); i++) {
                final Class clazz = types[i].getJavaType();
                try {
                    Object result = clazz.getMethod("valueOf", String.class).invoke(null, val);
                    // temporary workaround to work for Double as Double get parsed as Float with value infinity
                    if (!(result instanceof Float) || !((Float) result).isInfinite()) {
                        type = types[i];
                        break;
                    }
                } catch (Exception e) {
                    /* Exception is thrown if type does not match. Ignore to search next type */
                }
            }

            if (type == null) {
                type = STRING;
            }

            return type;
        }
    }

    /**
     * A custom JsonTypeIdResolver that uses the Field.Type property to deserialize
     * to the correct Schema.Field and/or its sub-classes.
     */
    static class SchemaJsonTypeIdResolver implements TypeIdResolver {
        private JavaType baseType;

        @Override
        public void init(JavaType javaType) {
            baseType = javaType;
        }

        @Override
        public String idFromValue(Object o) {
            return idFromValueAndType(o, o.getClass());
        }

        @Override
        public String idFromValueAndType(Object o, Class<?> aClass) {
            return null;
        }

        @Override
        public String idFromBaseType() {
            return idFromValueAndType(null, baseType.getRawClass());
        }

        @Override
        public JavaType typeFromId(String s) {
            return typeFromId(null, s);
        }

        @Override
        public JavaType typeFromId(DatabindContext databindContext, String s) {
            Type fieldType = Schema.Type.valueOf(s);
            JavaType javaType;
            switch (fieldType) {
                case NESTED:
                    javaType = TypeFactory.defaultInstance().constructType(NestedField.class);
                    break;
                case ARRAY:
                    javaType = TypeFactory.defaultInstance().constructType(ArrayField.class);
                    break;
                default:
                    javaType = TypeFactory.defaultInstance().constructType(Field.class);
            }
            return javaType;
        }

        @Override
        public JsonTypeInfo.Id getMechanism() {
            return JsonTypeInfo.Id.CUSTOM;
        }

        @Override
        public String getDescForKnownTypeIds() {
            return null;
        }
    }

    @JsonTypeInfo(use= JsonTypeInfo.Id.CUSTOM, include = JsonTypeInfo.As.PROPERTY, property = "type", visible = true)
    @JsonTypeIdResolver(SchemaJsonTypeIdResolver.class)
    public static class Field implements Serializable {
        String name;
        Type type;
        boolean optional;

        // for jackson
        public Field() {

        }

        public Field(Field other) {
            name = other.getName();
            type = other.getType();
            optional = other.isOptional();
        }

        public Field copy() {
            return new Field(this);
        }

        public static Field of(String name, Type type) {
            return new Field(name, type);
        }

        public static Field optional(String name, Type type) {
            return new Field(name, type, true);
        }

        // TODO: make it private after refactoring the usages
        public Field(String name, Type type){
            this(name, type, false);
        }

        private Field(String name, Type type, boolean optional){
            this.name = name;
            this.type = type;
            this.optional = optional;
        }

        public String getName(){
            return this.name;
        }

        public Type getType(){
            return this.type;
        }

        public boolean isOptional() {
            return optional;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Field field = (Field) o;

            if (optional != field.optional) return false;
            if (name != null ? !name.equals(field.name) : field.name != null) return false;
            return type == field.type;

        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (type != null ? type.hashCode() : 0);
            result = 31 * result + (optional ? 1 : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Field{" +
                    "name='" + name + '\'' +
                    ", type=" + type +
                    ", optional=" + optional +
                    '}';
        }

        // Input should be of the form: name='deviceId', type=LONG, optional
        public static Field fromString(String str) {
            String[] nameTypePair = str.split(",");
            String name = removePrimeSymbols(nameTypePair[0].split("=")[1]);
            String val = removePrimeSymbols(nameTypePair[1].split("=")[1]);
            boolean optional = nameTypePair.length >= 3 && nameTypePair[2].equalsIgnoreCase("optional");
            return new Field(name, Type.valueOf(val), optional);
        }

        // Removes the prime symbols that are in the beginning and end of the String,
        // e.g. 'device', device', 'device will be converted to device
        private static String removePrimeSymbols(String in) {
            return in.replaceAll("'?(\\w+)'?","$1");
        }
    }

    /**
     * A builder for constructing the schema from fields.
     */
    public static class SchemaBuilder {
        private final List<Field> fields = new ArrayList<>();
        public SchemaBuilder field(Field field) {
            fields.add(field);
            return this;
        }
        public SchemaBuilder fields(Field... fields) {
            Collections.addAll(this.fields, fields);
            return this;
        }

        public SchemaBuilder fields(List<Field> listOfFields) {
            this.fields.addAll(listOfFields);
            return this;
        }

        public Schema build() {
            if(fields.isEmpty()) {
                throw new IllegalArgumentException("Schema with empty fields!");
            }
            return new Schema(fields);
        }
    }

    /**
     * A composite type for representing nested types.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class NestedField extends Field {
        private String namespace;
        private List<Field> fields;

        public static NestedField of(String name, List<Field> fields) {
            return new NestedField(name, fields);
        }

        public static NestedField of(String name, String namespace,  Field... fields) {
            return new NestedField(name, namespace, Arrays.asList(fields), false);
        }

        public static NestedField of(String name, Field... fields) {
            return new NestedField(name, Arrays.asList(fields));
        }

        public static NestedField optional(String name, List<Field> fields) {
            return new NestedField(name, fields, true);
        }

        public static NestedField optional(String name, String namespace, Field... fields) {
            return new NestedField(name, namespace, Arrays.asList(fields), true);
        }

        public static NestedField optional(String name, Field... fields) {
            return new NestedField(name, Arrays.asList(fields), true);
        }

        private NestedField() {}

        private NestedField(String name, List<Field> fields) {
            this(name, fields, false);
        }

        private NestedField(String name, List<Field> fields, boolean optional) {
            this(name, null, fields, optional);
        }

        private NestedField(String name, String namespace, List<Field> fields, boolean optional) {
            super(name, Type.NESTED, optional);
            this.namespace = namespace;
            this.fields = ImmutableList.copyOf(fields);
        }

        public NestedField(NestedField other) {
            super(other);
            if (other.fields != null) {
                fields = other.fields.stream().map(Field::copy).collect(Collectors.toList());
            }
        }

        public NestedField copy() {
            return new NestedField(this);
        }

        public List<Field> getFields() {
            return fields;
        }

        public String getNamespace() {
            return namespace;
        }

        @Override
        public String toString() {
            return "NestedField{" +
                    "namespace='" + namespace + '\'' +
                    ", fields=" + fields +
                    '}' + super.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;

            NestedField that = (NestedField) o;

            if (namespace != null ? !namespace.equals(that.namespace) : that.namespace != null) return false;
            return fields != null ? fields.equals(that.fields) : that.fields == null;
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + (namespace != null ? namespace.hashCode() : 0);
            result = 31 * result + (fields != null ? fields.hashCode() : 0);
            return result;
        }
    }

    /**
     * A composite type that specifically represents an array or sequence of fields.
     */
    public static class ArrayField extends Field {
        /*
         * if members is a singleton it represents a homogeneous array of that type (e.g. Array[String])
         * if not a heterogeneous array like a JSON array.
         */
        private List<Field> members;

        public static ArrayField of(String name, List<Field> fields) {
            return new ArrayField(name, fields);
        }

        public static ArrayField of(String name, Field... fields) {
            return new ArrayField(name, Arrays.asList(fields));
        }

        public static ArrayField optional(String name, List<Field> fields) {
            return new ArrayField(name, fields, true);
        }

        public static ArrayField optional(String name, Field... fields) {
            return new ArrayField(name, Arrays.asList(fields), true);
        }

        // for jackson
        private ArrayField() {
        }

        private ArrayField(String name, List<Field> members) {
            this(name, members, false);
        }

        private ArrayField(String name, List<Field> members, boolean optional) {
            super(name, Type.ARRAY, optional);
            this.members = ImmutableList.copyOf(members);
        }

        public ArrayField(ArrayField other) {
            super(other);
            if (other.members != null) {
                members = other.members.stream().map(Field::copy).collect(Collectors.toList());
            }
        }

        public ArrayField copy() {
            return new ArrayField(this);
        }

        public List<Field> getMembers() {
            return members;
        }

        @JsonIgnore
        public boolean isHomogenous() {
            return members != null && members.size() == 1;
        }

        @Override
        public String toString() {
            return "ArrayField{" +
                    "name='" + name + '\'' +
                    "members=" + members +
                    "} ";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;

            ArrayField that = (ArrayField) o;

            return !(members != null ? !members.equals(that.members) : that.members != null);

        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + (members != null ? members.hashCode() : 0);
            return result;
        }
    }

    private final Map<String, Field> fields = new LinkedHashMap<>();

    // for jackson
    public Schema() {
    }

    // use the static factory or the builder
    private Schema(List<Field> fields){
        setFields(fields);
    }

    /**
     * Construct a new Schema of the given fields.
     */
    public static Schema of(Field... fields) {
        return new SchemaBuilder().fields(fields).build();
    }

    /**
     * Construct a new Schema of the given list of fields.
     */
    public static Schema of(List<Field> fields) {
        return new SchemaBuilder().fields(fields).build();
    }

    public static Schema unionOf(Schema first, Schema second) {
        List<Field> fields = new ArrayList<>();
        fields.addAll(first.getFields());
        fields.addAll(second.getFields());
        return new Schema(fields);
    }

    // for jackson
    public void setFields(List<Field> fields) {
        for (Field field: fields) {
            this.fields.put(field.getName().toUpperCase(), field);
        }
    }

    public List<Field> getFields(){
        return new ArrayList<>(this.fields.values());
    }

    /**
     * Returns a field in the schema with the given name or null
     * if the schema does not contain the field with the name.
     */
    public Field getField(String name) {
        return fields.get(name.toUpperCase());
    }

    //TODO: need to replace with actual ToJson from Json
    //TODO: this can be simplified to fields.toString() a
    public String toString() {
        if(fields == null) return "null";
        if(fields.isEmpty()) return "{}";
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for(Field field : fields.values()) {
            sb.append(field.toString()).append(",");
        }
        sb.setLength(sb.length() -1 );  // remove last, orphan ','
        return sb.append("}").toString();
    }

    // input received is typically of the form {{name='deviceId', type=LONG},{name='deviceName', type=STRING},}
    public static Schema fromString(String str) {
        if (str.equals("null")) {
            return null;
        }

        if (str.equals("{}")) {
            return new Schema(new ArrayList<Field>());
        }

        str = str.replace(",}", ",");   // remove the last orphan ',' in inputs such as {{name='deviceName', type=STRING},}
        str = str.replace("{", "");
        str = str.replace("{", "");
        str = str.replace("}}", "");    // remove }} at the end of the String

        String[] split = str.split("},");
        List<Field> fields = new ArrayList<>();
        for(String fieldStr : split) {
            fields.add(Field.fromString(fieldStr));
        }
        return new Schema(fields);
    }


    /**
     * Constructs a schema object from a map of sample data.
     *
     * @param parsedData
     * @return
     * @throws ParserException
     */
    public static Schema fromMapData(Map<String, Object> parsedData) throws ParserException {
        List<Field> fields = parseFields(parsedData);
        return new SchemaBuilder().fields(fields).build();
    }

    private static List<Field> parseFields(Map<String, Object> fieldMap) throws ParserException {
        List<Field> fields = new ArrayList<>();
        for(Map.Entry<String, Object> entry: fieldMap.entrySet()) {
            fields.add(parseField(entry.getKey(), entry.getValue()));
        }
        return fields;
    }

    private static Field parseField(String fieldName, Object fieldValue) throws ParserException {
        Field field = null;
        Type fieldType = fromJavaType(fieldValue);
        if(fieldType == Type.NESTED) {
            field = new NestedField(fieldName, parseFields((Map<String, Object>)fieldValue));
        } else if(fieldType == Type.ARRAY) {
            Multiset<Field> members = parseArray((List<Object>)fieldValue);
            Set<Field> fieldTypes = members.elementSet();
            if (fieldTypes.size() > 1) {
                field = new ArrayField(fieldName, new ArrayList<>(members));
            } else if (fieldTypes.size() == 1) {
                field = new ArrayField(fieldName, new ArrayList<>(members.elementSet()));
            } else {
                throw new IllegalArgumentException("Array should have at least one element");
            }
        } else {
            field = new Field(fieldName, fieldType);
        }
        return field;
    }

    private static Multiset<Field> parseArray(List<Object> array) throws ParserException {
        Multiset<Field> members = LinkedHashMultiset.create();
        for(Object member: array) {
            members.add(parseField(null, member));
        }
        return members;
    }

    //TODO: complete this and move into some parser utility class
    public static Type fromJavaType(Object value) throws ParserException {
        if(value instanceof String) {
            return Type.STRING;
        } else if (value instanceof Short) {
            return Type.SHORT;
        } else if (value instanceof Byte) {
            return Type.BYTE;
        } else if (value instanceof Float) {
            return Type.FLOAT;
        } else if (value instanceof Long) {
            return Type.LONG;
        } else if (value instanceof Double) {
            return Type.DOUBLE;
        } else if (value instanceof Integer) {
            return Type.INTEGER;
        } else if (value instanceof Boolean) {
            return Type.BOOLEAN;
        } else if (value instanceof byte[]) {
            return Type.BINARY;
        } else if (value instanceof InputStream) {
            return Type.BLOB;
        } else if (value instanceof List) {
            return Type.ARRAY;
        } else if (value instanceof Map) {
            return Type.NESTED;
        }

        throw new ParserException("Unknown type " + value.getClass());
    }

    public static Type fromJavaType(Class<?> clazz) throws ParserException {
        if(clazz.equals(String.class)) {
            return Type.STRING;
        } else if (clazz.equals(Short.class)) {
            return Type.SHORT;
        } else if (clazz.equals(Byte.class)) {
            return Type.BYTE;
        } else if (clazz.equals(Float.class)) {
            return Type.FLOAT;
        } else if (clazz.equals(Long.class)) {
            return Type.LONG;
        } else if (clazz.equals(Double.class)) {
            return Type.DOUBLE;
        } else if (clazz.equals(Integer.class)) {
            return Type.INTEGER;
        } else if (clazz.equals(Boolean.class)) {
            return Type.BOOLEAN;
        } else if (clazz.equals(byte[].class)) {
            return Type.BINARY;
        } else if (clazz.equals(InputStream.class)) {
            return Type.BLOB;
        } else if (clazz.equals(List.class)) {
            return Type.ARRAY;
        } else if (clazz.equals(Map.class)) {
            return Type.NESTED;
        }

        throw new ParserException("Unknown type " + clazz);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Schema schema = (Schema) o;

        return !(fields != null ? !fields.equals(schema.fields) : schema.fields != null);

    }

    @Override
    public int hashCode() {
        return fields != null ? fields.hashCode() : 0;
    }
}
