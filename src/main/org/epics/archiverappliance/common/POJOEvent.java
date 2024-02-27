package org.epics.archiverappliance.common;

import edu.stanford.slac.archiverappliance.PB.data.DefaultEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.SampleValue;

import java.time.Instant;
import java.util.Map;

/**
 * A simple POJO that implements the event interface.
 * Use for converting into the PB classes especially in the post processors.
 * @author mshankar
 *
 */
public class POJOEvent implements DBRTimeEvent {
    public static Logger logger = LogManager.getLogger(POJOEvent.class.getName());
    private final DefaultEvent defaultEvent;

    public POJOEvent(
            ArchDBRTypes dbrType, Instant recordProcessingTime, SampleValue sampleValue, int status, int severity) {
        this.defaultEvent =
                new DefaultEvent(recordProcessingTime, severity, status, Map.of(), false, sampleValue, dbrType);
    }

    public POJOEvent(
            ArchDBRTypes dbrType, Instant recordProcessingTime, String sampleValueStr, int status, int severity) {
        this(
                dbrType,
                recordProcessingTime,
                ArchDBRTypes.sampleValueFromString(dbrType, sampleValueStr),
                status,
                severity);
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
