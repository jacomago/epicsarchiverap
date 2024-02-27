/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.data;

import org.epics.archiverappliance.config.ArchDBRTypes;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Get the value of this event.
 * Within the archiver appliance, we only ask some form of converting to a string and perhaps a Number.
 * The toString for vectors generates a JSON form of the vector...
 * @author mshankar
 */
public interface SampleValue {
    String toString();

    int getElementCount();

    Number getValue();

    Number getValue(int index);

    String getStringValue(int index);

    String toJSONString();

    @SuppressWarnings("rawtypes")
    List getValues();

    List<String> getStringValues();

    <T extends Number> List<T> getNumberValues();
    /**
     * Return the value as a ByteBuffer that is ready to read.
     * @return ByteBuffer  &emsp;
     */
    ByteBuffer getValueAsBytes();

    static String toCSVString(SampleValue val, ArchDBRTypes type) throws IOException {
        if (val == null) return "";

        switch (type) {
            case DBR_SCALAR_STRING:
            case DBR_SCALAR_SHORT:
            case DBR_SCALAR_FLOAT:
            case DBR_SCALAR_ENUM:
            case DBR_SCALAR_BYTE:
            case DBR_SCALAR_INT:
            case DBR_SCALAR_DOUBLE:
            case DBR_V4_GENERIC_BYTES:
                return val.toString();
            case DBR_WAVEFORM_STRING:
            case DBR_WAVEFORM_ENUM: {
                int elementCount = val.getElementCount();
                boolean first = true;
                StringWriter buf = new StringWriter();
                for (int i = 0; i < elementCount; i++) {
                    String elemVal = val.getStringValue(i);
                    if (first) {
                        first = false;
                    } else {
                        buf.append("|");
                    }
                    buf.append(elemVal);
                }
                return buf.toString();
            }
            case DBR_WAVEFORM_SHORT:
            case DBR_WAVEFORM_FLOAT:
            case DBR_WAVEFORM_BYTE:
            case DBR_WAVEFORM_INT:
            case DBR_WAVEFORM_DOUBLE: {
                int elementCount = val.getElementCount();
                boolean first = true;
                StringWriter buf = new StringWriter();
                for (int i = 0; i < elementCount; i++) {
                    String elemVal = val.getValue(i).toString();
                    if (first) {
                        first = false;
                    } else {
                        buf.append("|");
                    }
                    buf.append(elemVal);
                }
                return buf.toString();
            }
            default:
                throw new IOException("Unsupported DBR type in switch statement " + type);
        }
    }
}
