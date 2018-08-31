package com.trivadis.kafka.smt;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public class ValueToTimestamp<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC = "Replace the record key with a new key formed from a subset of fields in the record value.";

    public static final String FIELD_CONFIG = "field";
    private static final String FIELD_DEFAULT = "";
    
    public static final String FORMAT_CONFIG = "format";
    private static final String FORMAT_DEFAULT = "";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_CONFIG, ConfigDef.Type.STRING, FIELD_DEFAULT, ConfigDef.Importance.HIGH,
                    "Field name on the record value to extract as the record timestamp.") 
            .define(FORMAT_CONFIG, ConfigDef.Type.STRING, FORMAT_DEFAULT, ConfigDef.Importance.MEDIUM,
                            "A SimpleDateFormat-compatible format for the timestamp. Used to generate the output when type=string "
                                    + "or used to parse the input if the input is a string.");

    private static final String PURPOSE = "copying fields from value to timestamp";

    private static final String TYPE_STRING = "string";
    private static final String TYPE_UNIX = "unix";
    private static final String TYPE_DATE = "Date";
    private static final String TYPE_TIME = "Time";
    private static final String TYPE_TIMESTAMP = "Timestamp";
    private static final Set<String> VALID_TYPES = new HashSet<>(Arrays.asList(TYPE_STRING, TYPE_UNIX, TYPE_DATE, TYPE_TIME, TYPE_TIMESTAMP));

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    
    private interface TimestampTranslator {
        /**
         * Convert from the type-specific format to the universal java.util.Date format
         */
        Date toRaw(Config config, Object orig);

        /**
         * Get the schema for this format.
         */
        Schema typeSchema();

        /**
         * Convert from the universal java.util.Date format to the type-specific format
         */
        Object toType(Config config, Date orig);
}    
    
    private static final Map<String, TimestampTranslator> TRANSLATORS = new HashMap<>();
    static {
        TRANSLATORS.put(TYPE_STRING, new TimestampTranslator() {
            @Override
            public Date toRaw(Config config, Object orig) {
                if (!(orig instanceof String))
                    throw new DataException("Expected string timestamp to be a String, but found " + orig.getClass());
                try {
                    return config.format.parse((String) orig);
                } catch (ParseException e) {
                    throw new DataException("Could not parse timestamp: value (" + orig + ") does not match pattern ("
                            + config.format.toPattern() + ")", e);
                }
            }

            @Override
            public Schema typeSchema() {
                return Schema.STRING_SCHEMA;
            }

            @Override
            public String toType(Config config, Date orig) {
                synchronized (config.format) {
                    return config.format.format(orig);
                }
            }
        });

        TRANSLATORS.put(TYPE_UNIX, new TimestampTranslator() {
            @Override
            public Date toRaw(Config config, Object orig) {
                if (!(orig instanceof Long))
                    throw new DataException("Expected Unix timestamp to be a Long, but found " + orig.getClass());
                return Timestamp.toLogical(Timestamp.SCHEMA, (Long) orig);
            }

            @Override
            public Schema typeSchema() {
                return Schema.INT64_SCHEMA;
            }

            @Override
            public Long toType(Config config, Date orig) {
                return Timestamp.fromLogical(Timestamp.SCHEMA, orig);
            }
        });

        TRANSLATORS.put(TYPE_DATE, new TimestampTranslator() {
            @Override
            public Date toRaw(Config config, Object orig) {
                if (!(orig instanceof Date))
                    throw new DataException("Expected Date to be a java.util.Date, but found " + orig.getClass());
                // Already represented as a java.util.Date and Connect Dates are a subset of valid java.util.Date values
                return (Date) orig;
            }

            @Override
            public Schema typeSchema() {
                return org.apache.kafka.connect.data.Date.SCHEMA;
            }

            @Override
            public Date toType(Config config, Date orig) {
                Calendar result = Calendar.getInstance(UTC);
                result.setTime(orig);
                result.set(Calendar.HOUR_OF_DAY, 0);
                result.set(Calendar.MINUTE, 0);
                result.set(Calendar.SECOND, 0);
                result.set(Calendar.MILLISECOND, 0);
                return result.getTime();
            }
        });

        TRANSLATORS.put(TYPE_TIME, new TimestampTranslator() {
            @Override
            public Date toRaw(Config config, Object orig) {
                if (!(orig instanceof Date))
                    throw new DataException("Expected Time to be a java.util.Date, but found " + orig.getClass());
                // Already represented as a java.util.Date and Connect Times are a subset of valid java.util.Date values
                return (Date) orig;
            }

            @Override
            public Schema typeSchema() {
                return Time.SCHEMA;
            }

            @Override
            public Date toType(Config config, Date orig) {
                Calendar origCalendar = Calendar.getInstance(UTC);
                origCalendar.setTime(orig);
                Calendar result = Calendar.getInstance(UTC);
                result.setTimeInMillis(0L);
                result.set(Calendar.HOUR_OF_DAY, origCalendar.get(Calendar.HOUR_OF_DAY));
                result.set(Calendar.MINUTE, origCalendar.get(Calendar.MINUTE));
                result.set(Calendar.SECOND, origCalendar.get(Calendar.SECOND));
                result.set(Calendar.MILLISECOND, origCalendar.get(Calendar.MILLISECOND));
                return result.getTime();
            }
        });

        TRANSLATORS.put(TYPE_TIMESTAMP, new TimestampTranslator() {
            @Override
            public Date toRaw(Config config, Object orig) {
                if (!(orig instanceof Date))
                    throw new DataException("Expected Timestamp to be a java.util.Date, but found " + orig.getClass());
                return (Date) orig;
            }

            @Override
            public Schema typeSchema() {
                return Timestamp.SCHEMA;
            }

            @Override
            public Date toType(Config config, Date orig) {
                return orig;
            }
        });
    }

    // This is a bit unusual, but allows the transformation config to be passed to static anonymous classes to customize
    // their behavior
    private static class Config {
        Config(String field, String type, SimpleDateFormat format) {
            this.field = field;
            this.type = type;
            this.format = format;
        }
        String field;
        String type;
        SimpleDateFormat format;
    }
    private Config config;

    private Cache<Schema, Schema> valueToTimestampSchemaCache;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig simpleConfig = new SimpleConfig(CONFIG_DEF, configs);
        final String field = simpleConfig.getString(FIELD_CONFIG);
        final String formatPattern = simpleConfig.getString(FORMAT_CONFIG);
        valueToTimestampSchemaCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
        
//       if (type.equals(TYPE_STRING) && formatPattern.trim().isEmpty()) {
//            throw new ConfigException("TimestampConverter requires format option to be specified when using string timestamps");
//        }
 
        SimpleDateFormat format = null;
        if (formatPattern != null && !formatPattern.trim().isEmpty()) {
            try {
                format = new SimpleDateFormat(formatPattern);
                format.setTimeZone(UTC);
            } catch (IllegalArgumentException e) {
                throw new ConfigException("TimestampConverter requires a SimpleDateFormat-compatible pattern for string timestamps: "
                        + formatPattern, e);
            }
}
        config = new Config(field, TYPE_UNIX, format);

        
    }

    @Override
    public R apply(R record) {
        if (record.valueSchema() == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(record.value(), PURPOSE);
        final long timestamp = (Long)convertTimestamp(value.get(config.field));

        return record.newRecord(record.topic(), record.kafkaPartition(), null, record.key(), record.valueSchema(), record.value(), timestamp);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(record.value(), PURPOSE);
        final long timestamp = (Long)convertTimestamp(value.get(config.field));  // timestampTypeFromSchema(field.schema())

        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), value.schema(), value, timestamp);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        valueToTimestampSchemaCache = null;
    }

    /**
     * Infer the type/format of the timestamp based on the raw Java type
     */
    private String inferTimestampType(Object timestamp) {
        // Note that we can't infer all types, e.g. Date/Time/Timestamp all have the same runtime representation as a
        // java.util.Date
        if (timestamp instanceof Date) {
            return TYPE_TIMESTAMP;
        } else if (timestamp instanceof Long) {
            return TYPE_UNIX;
        } else if (timestamp instanceof String) {
            return TYPE_STRING;
        }
        throw new DataException("TimestampConverter does not support " + timestamp.getClass() + " objects as timestamps");
    }    
   
   /** 
    * Convert the given timestamp to the target timestamp format.
    * @param timestamp the input timestamp
    * @param timestampFormat the format of the timestamp, or null if the format should be inferred
    * @return the converted timestamp
    */
   private Object convertTimestamp(Object timestamp, String timestampFormat) {
       if (timestampFormat == null) {
           timestampFormat = inferTimestampType(timestamp);
       }

       TimestampTranslator sourceTranslator = TRANSLATORS.get(timestampFormat);
       if (sourceTranslator == null) {
           throw new ConnectException("Unsupported timestamp type: " + timestampFormat);
       }
       Date rawTimestamp = sourceTranslator.toRaw(config, timestamp);

       TimestampTranslator targetTranslator = TRANSLATORS.get(config.type);
       if (targetTranslator == null) {
           throw new ConnectException("Unsupported timestamp type: " + config.type);
       }
       return targetTranslator.toType(config, rawTimestamp);
   }

   private Object convertTimestamp(Object timestamp) {
       return convertTimestamp(timestamp, null);
   }

}
