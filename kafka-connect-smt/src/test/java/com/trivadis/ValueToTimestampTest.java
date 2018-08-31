package com.trivadis;


import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import com.trivadis.kafka.smt.ValueToTimestamp;

import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class ValueToTimestampTest {
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    private static final Calendar EPOCH;
    private static final Calendar TIME;
    private static final Calendar DATE;
    private static final Calendar DATE_PLUS_TIME;
    private static final Long DATE_PLUS_TIME_UNIX;
    private static final String STRING_DATE_FMT = "yyyy MM dd HH mm ss SSS z";
    private static final String DATE_PLUS_TIME_STRING;

    private final ValueToTimestamp<SinkRecord> xformValue = new ValueToTimestamp<>();

    static {
        EPOCH = GregorianCalendar.getInstance(UTC);
        EPOCH.setTimeInMillis(0L);

        TIME = GregorianCalendar.getInstance(UTC);
        TIME.setTimeInMillis(0L);
        TIME.add(Calendar.MILLISECOND, 1234);

        DATE = GregorianCalendar.getInstance(UTC);
        DATE.setTimeInMillis(0L);
        DATE.set(1970, Calendar.JANUARY, 1, 0, 0, 0);
        DATE.add(Calendar.DATE, 1);

        DATE_PLUS_TIME = GregorianCalendar.getInstance(UTC);
        DATE_PLUS_TIME.setTimeInMillis(0L);
        DATE_PLUS_TIME.add(Calendar.DATE, 1);
        DATE_PLUS_TIME.add(Calendar.MILLISECOND, 1234);

        DATE_PLUS_TIME_UNIX = new Long(DATE_PLUS_TIME.getTime().getTime());
        DATE_PLUS_TIME_STRING = "1970 01 02 00 00 01 234 UTC";
    }


    // Configuration

    @After
    public void teardown() {
        xformValue.close();
    }

    // Conversions without schemas (most flexible Timestamp -> other types)

    @Test
    public void schemaless() {
    	Map<String, String> config = new HashMap<>();
        config.put(ValueToTimestamp.FIELD_CONFIG, "ts");
        config.put(ValueToTimestamp.FORMAT_CONFIG, STRING_DATE_FMT);
        xformValue.configure(config);

        final HashMap<String, Object> value = new HashMap<>();
        value.put("ts", DATE_PLUS_TIME_STRING);
        value.put("other", 2);

        final SinkRecord record = new SinkRecord("", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xformValue.apply(record);
    }
    
    @Test
    public void withSchema() {
    	Map<String, String> config = new HashMap<>();
        config.put(ValueToTimestamp.FIELD_CONFIG, "ts");
        config.put(ValueToTimestamp.FORMAT_CONFIG, STRING_DATE_FMT);
        xformValue.configure(config);

        final Schema valueSchema = SchemaBuilder.struct()
                .field("ts", Schema.STRING_SCHEMA)
                .field("other", Schema.INT32_SCHEMA)
                .build();

        final Struct value = new Struct(valueSchema);
        value.put("ts", DATE_PLUS_TIME_STRING);
        value.put("other", 2);

        final SinkRecord record = new SinkRecord("", 0, null, null, valueSchema, value, 0);
        final SinkRecord transformedRecord = xformValue.apply(record);

//        assertEquals(Timestamp.SCHEMA, transformedRecord.timestampType());
        assertEquals(DATE_PLUS_TIME_UNIX, transformedRecord.timestamp());       
    }

}
