/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.channelarchiver;

import edu.stanford.slac.archiverappliance.PB.data.DefaultEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.data.ScalarStringSampleValue;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.data.VectorStringSampleValue;
import org.epics.archiverappliance.data.VectorValue;

import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * We get a HashMap of NVPairs from the Channel Archiver - this class exposes these as an archiver Event
 * We typically get secs=1250696265, value=70.9337, sevr=0, nano=267115322, stat=0
 * @author mshankar
 *
 */
public class HashMapEvent implements DBRTimeEvent {
    public static Logger logger = LogManager.getLogger(HashMapEvent.class.getName());

    public static final String SECS_FIELD_NAME = "secs";
    public static final String NANO_FIELD_NAME = "nano";
    public static final String VALUE_FIELD_NAME = "value";
    public static final String STAT_FIELD_NAME = "stat";
    public static final String SEVR_FIELD_NAME = "sevr";
    public static final String FIELD_VALUES_FIELD_NAME = "fields";
    public static final String FIELD_VALUES_ACTUAL_CHANGE = "fieldsAreActualChange";

    private final DefaultEvent<?> defaultEvent;

    public HashMapEvent(ArchDBRTypes type, HashMap<String, Object> values) {
        this.defaultEvent = new DefaultEvent<>(
                Instant.ofEpochSecond(Long.parseLong((String) values.get(SECS_FIELD_NAME)), Integer.parseInt((String)
                        values.get(NANO_FIELD_NAME))),
                Integer.parseInt((String) values.get(SEVR_FIELD_NAME)),
                Integer.parseInt((String) values.get(STAT_FIELD_NAME)),
                (Map<String, String>) values.get(FIELD_VALUES_FIELD_NAME),
                values.containsKey(FIELD_VALUES_ACTUAL_CHANGE)
                        && Boolean.parseBoolean((String) values.get(FIELD_VALUES_ACTUAL_CHANGE)),
                fieldToSampleValue(type, values.get(VALUE_FIELD_NAME)),
                type);
    }

    public HashMapEvent(DBRTimeEvent event) {
        this.defaultEvent = DefaultEvent.fromDBREvent(event);
    }

    private static SampleValue fieldToSampleValue(ArchDBRTypes type, Object value) {

        switch (type) {
            case DBR_SCALAR_FLOAT:
            case DBR_SCALAR_DOUBLE: {
                String strValue = (String) value;
                try {
                    return new ScalarValue<Double>(Double.parseDouble(strValue));
                } catch (NumberFormatException nex) {
                    if (strValue.equals("nan")) {
                        // logger.debug("Got a nan from the ChannelArchiver; returning Double.Nan instead");
                        return new ScalarValue<Double>(Double.NaN);
                    } else {
                        throw nex;
                    }
                }
            }
            case DBR_SCALAR_BYTE:
            case DBR_SCALAR_SHORT:
            case DBR_SCALAR_ENUM:
            case DBR_SCALAR_INT: {
                String strValue = (String) value;
                return new ScalarValue<Integer>(Integer.parseInt(strValue));
            }
            case DBR_SCALAR_STRING: {
                String strValue = (String) value;
                return new ScalarStringSampleValue(strValue);
            }
            case DBR_WAVEFORM_FLOAT:
            case DBR_WAVEFORM_DOUBLE: {
                // No choice but to add this SuppressWarnings here.
                @SuppressWarnings("unchecked")
                LinkedList<String> vals = (LinkedList<String>) value;
                LinkedList<Double> dvals = new LinkedList<Double>();
                for (String val : vals) dvals.add(Double.parseDouble(val));
                return new VectorValue<Double>(dvals);
            }
            case DBR_WAVEFORM_ENUM:
            case DBR_WAVEFORM_SHORT:
            case DBR_WAVEFORM_BYTE:
            case DBR_WAVEFORM_INT: {
                // No choice but to add this SuppressWarnings here.
                @SuppressWarnings("unchecked")
                LinkedList<String> vals = (LinkedList<String>) value;
                LinkedList<Integer> ivals = new LinkedList<Integer>();
                for (String val : vals) ivals.add(Integer.parseInt(val));
                return new VectorValue<Integer>(ivals);
            }
            case DBR_WAVEFORM_STRING: {
                // No choice but to add this SuppressWarnings here.
                @SuppressWarnings("unchecked")
                LinkedList<String> vals = (LinkedList<String>) value;
                return new VectorStringSampleValue(vals);
            }
            case DBR_V4_GENERIC_BYTES: {
                throw new UnsupportedOperationException("Channel Archiver does not support V4 yet.");
            }
            default:
                throw new UnsupportedOperationException("Unknown DBR type " + type);
        }
    }


    @Override
    public SampleValue getSampleValue() {
        return this.defaultEvent.value();
    }

    @Override
    public Instant getEventTimeStamp() {
        return this.defaultEvent.getEventTimeStamp();
    }

    @Override
    public int getStatus() {
        return this.defaultEvent.getStatus();
    }

    @Override
    public int getSeverity() {
        return this.defaultEvent.severity();
    }

    @Override
    public int getRepeatCount() {
        return this.defaultEvent.getRepeatCount();
    }

    @Override
    public long getEpochSeconds() {
        return this.defaultEvent.getEpochSeconds();
    }

    @Override
    public ByteArray getRawForm() {
        return this.defaultEvent.getRawForm();
    }

    @Override
    public boolean hasFieldValues() {
        return this.defaultEvent.hasFieldValues();
    }

    @Override
    public boolean isActualChange() {
        return this.defaultEvent.isActualChange();
    }

    @Override
    public Map<String, String> getFields() {
        return this.defaultEvent.getFields();
    }

    @Override
    public String getFieldValue(String fieldName) {
        return this.defaultEvent.getFieldValue(fieldName);
    }

    @Override
    public DBRTimeEvent cloneWithExtraFieldValues(Map<String, String> fieldValues, boolean actualChange) {
        return this.defaultEvent.cloneWithExtraFieldValues(fieldValues, actualChange);
    }

    @Override
    public ArchDBRTypes getDBRType() {
        return this.defaultEvent.getDBRType();
    }
}
