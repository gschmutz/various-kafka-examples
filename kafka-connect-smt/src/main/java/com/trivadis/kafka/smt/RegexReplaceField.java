/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.trivadis.kafka.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class RegexReplaceField<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Replace specified fields with a valid null value for the field type (i.e. 0, false, empty string, and so on)."
                    + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName() + "</code>) "
                    + "or value (<code>" + Value.class.getName() + "</code>).";

    private static final String FIELDS_CONFIG = "fields";

    private static final String REGEX_CONFIG = "regex";
    private static final String REGEX_DEFAULT = "";

    private static final String REPLACEMENT_CONFIG = "replacement";
    private static final String REPLACEMENT_DEFAULT = "";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyListValidator(), ConfigDef.Importance.HIGH, 
            			"Names of fields to replace.")
    		.define(REGEX_CONFIG, ConfigDef.Type.STRING, REGEX_DEFAULT, ConfigDef.Importance.HIGH, 
    					"The regular expression to search for.")
			.define(REPLACEMENT_CONFIG, ConfigDef.Type.STRING, REPLACEMENT_DEFAULT, ConfigDef.Importance.HIGH, 
						"The replacement value.");

    private static final String PURPOSE = "replace fields";

     
    private Set<String> replacedFields;
    private String regex;
    private String replacement; 

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        replacedFields = new HashSet<>(config.getList(FIELDS_CONFIG));
        regex = config.getString(REGEX_CONFIG);
        replacement = config.getString(REPLACEMENT_CONFIG);
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final HashMap<String, Object> updatedValue = new HashMap<>(value);
        for (String field : replacedFields) {
            updatedValue.put(field, replaced(value.get(field)));
        }
        return newRecord(record, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        final Struct updatedValue = new Struct(value.schema());
        for (Field field : value.schema().fields()) {
            final Object origFieldValue = value.get(field);
            updatedValue.put(field, replacedFields.contains(field.name()) ? replaced(origFieldValue) : origFieldValue);
        }
        return newRecord(record, updatedValue);
    }

    private Object replaced(Object value) {
        if (value == null)
            return null;
        Object maskedValue = ((String) value).replaceAll(regex, replacement);
        if (maskedValue == null) {
            if (value instanceof List)
                maskedValue = Collections.emptyList();
            else if (value instanceof Map)
                maskedValue = Collections.emptyMap();
            else
                throw new DataException("Cannot mask value of type: " + value.getClass());
        }
        return maskedValue;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R base, Object value);

    public static final class Key<R extends ConnectRecord<R>> extends RegexReplaceField<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static final class Value<R extends ConnectRecord<R>> extends RegexReplaceField<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), updatedValue, record.timestamp());
        }
    }

}
