/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.config.ArchDBRTypes;

import java.io.IOException;

/**
 * A POJO that encapsulates all the information needed about a stream from an ETL source
 * @author mshankar
 *
 */
public class ETLInfo {
    private final String pvName;
    private final String key;
    private final Event firstEvent;
    private final PartitionGranularity granularity;
    private ETLStreamCreator strmCreator;
    private final ArchDBRTypes type;
    private final long size;

    public ETLInfo(
            String pvName,
            ArchDBRTypes type,
            String key,
            PartitionGranularity granularity,
            ETLStreamCreator strmCreator,
            Event firstEvent,
            long size) {
        this.pvName = pvName;
        this.type = type;
        this.key = key;
        this.granularity = granularity;
        this.strmCreator = strmCreator;
        this.firstEvent = firstEvent;
        this.size = size;
    }

    public ArchDBRTypes getType() {
        return type;
    }

    public String getPvName() {
        return pvName;
    }

    public String getKey() {
        return key;
    }

    public PartitionGranularity getGranularity() {
        return granularity;
    }

    public EventStream getEv() throws IOException {
        return strmCreator.getStream();
    }

    public Event getFirstEvent() {
        return firstEvent;
    }

    public long getSize() {
        return size;
    }

    public ETLStreamCreator getStrmCreator() {
        return strmCreator;
    }

    public void setStrmCreator(ETLStreamCreator strmCreator) {
        this.strmCreator = strmCreator;
    }
}
