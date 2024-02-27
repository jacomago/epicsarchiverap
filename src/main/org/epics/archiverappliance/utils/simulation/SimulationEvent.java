/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.utils.simulation;

import edu.stanford.slac.archiverappliance.PB.data.DefaultEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.SampleValue;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * An event typically used in the unit tests.
 * @author mshankar
 *
 */
public class SimulationEvent implements DBRTimeEvent {
    private final DefaultEvent<?> defaultEvent;

    public SimulationEvent(Instant instant, ArchDBRTypes type, SampleValue value) {
        this.defaultEvent = new DefaultEvent<>(instant, 0, 0, Map.of(), false, value, type);
    }

    public SimulationEvent(Instant instant, ArchDBRTypes type, SimulationValueGenerator valueGenerator) {
        this(
                instant,
                type,
                valueGenerator.getSampleValue(type, TimeUtils.getSecondsIntoYear(instant.getEpochSecond())));
    }

    public SimulationEvent(YearSecondTimestamp yts, ArchDBRTypes type, SampleValue sampleValue) {
        this(TimeUtils.convertFromYearSecondTimestamp(yts), type, sampleValue);
    }

    public SimulationEvent(int secondsIntoYear, short yearofdata, ArchDBRTypes type, SampleValue sampleValue) {
        this(new YearSecondTimestamp(yearofdata, secondsIntoYear, 0), type, sampleValue);
    }

    public DefaultEvent<?> getDefaultEvent() {
        return this.defaultEvent;
    }

    public SimulationEvent(SimulationEvent src) {
        this.defaultEvent = src.getDefaultEvent();
    }


    @Override
    public SampleValue getSampleValue() {
        return this.defaultEvent.value();
    }

    @Override
    public Instant getEventTimeStamp() {
        return this.defaultEvent.instant();
    }

    @Override
    public int getStatus() {
        return 0;
    }

    @Override
    public int getSeverity() {
        return 0;
    }

    @Override
    public int getRepeatCount() {
        return 0;
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

    public int getNanos() {
        return this.defaultEvent.instant().getNano();
    }
}
