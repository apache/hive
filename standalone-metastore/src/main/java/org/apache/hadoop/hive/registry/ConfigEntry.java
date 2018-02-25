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
package org.apache.hadoop.hive.registry;

import java.io.Serializable;
import java.util.Collections;

/**
 * Details about a configuration entry containing
 * - name, type, description, mandatory, default value and validator.
 * <p>
 * Mandatory configuration entry can be created with {@link ConfigEntry#mandatory(String, Class, String, Object, Validator)}
 * Optional configuration entry can be created with {@link ConfigEntry#optional(String, Class, String, Object, Validator)}
 */
public final class ConfigEntry<T> implements Serializable {

    private static final long serialVersionUID = -3408548579276359761L;

    private final String name;
    private final Class<? extends T> type;
    private final String description;
    private final boolean mandatory;
    private final T defaultValue;
    private final Validator<? extends T> validator;

    private ConfigEntry(String name,
                        Class<? extends T> type,
                        String description,
                        boolean mandatory,
                        T defaultValue,
                        Validator<? extends T> validator) {
        this.name = name;
        this.type = type;
        this.description = description;
        this.mandatory = mandatory;
        this.defaultValue = defaultValue;
        this.validator = validator;
    }

    public static <T> ConfigEntry<T> mandatory(String name,
                                               Class<? extends T> type,
                                               String description,
                                               T defaultValue,
                                               Validator<? extends T> validator) {
        return new ConfigEntry<T>(name, type, description, true, defaultValue, validator);
    }

    public static <T> ConfigEntry<T> optional(String name,
                                              Class<? extends T> type,
                                              String description,
                                              T defaultValue,
                                              Validator<? extends T> validator) {
        return new ConfigEntry<T>(name, type, description, false, defaultValue, validator);
    }

    public String name() {
        return name;
    }

    public Class<? extends T> type() {
        return type;
    }

    public String description() {
        return description;
    }

    public T defaultValue() {
        return defaultValue;
    }

    public Validator<? extends T> validator() {
        return validator;
    }

    public boolean isMandatory() {
        return mandatory;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConfigEntry<?> that = (ConfigEntry<?>) o;

        if (mandatory != that.mandatory) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        if (description != null ? !description.equals(that.description) : that.description != null) return false;
        if (defaultValue != null ? !defaultValue.equals(that.defaultValue) : that.defaultValue != null) return false;
        return validator != null ? validator.equals(that.validator) : that.validator == null;

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (mandatory ? 1 : 0);
        result = 31 * result + (defaultValue != null ? defaultValue.hashCode() : 0);
        result = 31 * result + (validator != null ? validator.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ConfigEntry{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", description='" + description + '\'' +
                ", mandatory=" + mandatory +
                ", defaultValue=" + defaultValue +
                ", validator=" + validator +
                '}';
    }

    /**
     * Validates the given value for a given {@link ConfigEntry}
     *
     * @param <V> type of the value.
     */
    public interface Validator<V> {
        /**
         * Validates the given value
         *
         * @param v type of the value
         *
         * @throws IllegalArgumentException when the given value is not valid.
         */
        void validate(V v);

        /**
         * Multiple {@link Validator}s can be composed with this method.
         *
         * @param other other validator to be composed with this validator
         *
         * @return Returns the current Validator instance.
         */
        default Validator<V> with(final Validator<V> other) {
            return with(Collections.singletonList(other));
        }

        /**
         * Multiple {@link Validator}s can be composed with this method.
         *
         * @param others other validators to be composed with this validator
         *
         * @return Returns the current Validator instance.
         */
        default Validator<V> with(final Iterable<Validator<V>> others) {
            return v -> {
                this.validate(v);
                for (Validator<V> other : others) {
                    other.validate(v);
                }
            };
        }
    }

    public static class NonEmptyStringValidator implements ConfigEntry.Validator<String> {

        private static ConfigEntry.Validator<String> instance = new NonEmptyStringValidator();

        @Override
        public void validate(String str) {
            if (str == null || str.isEmpty()) {
                throw new IllegalArgumentException("Given string " + str + " must be non empty");
            }
        }

        public static ConfigEntry.Validator<String> get() {
            return instance;
        }
    }

    public static class PositiveNumberValidator implements ConfigEntry.Validator<Number> {
        private static final PositiveNumberValidator instance = new PositiveNumberValidator();

        @Override
        public void validate(Number number) {
            if (number.doubleValue() <= 0) {
                throw new IllegalArgumentException("Given number " + number + " must be greater than zero.");
            }
        }

        public static PositiveNumberValidator get() {
            return instance;
        }
    }


}
