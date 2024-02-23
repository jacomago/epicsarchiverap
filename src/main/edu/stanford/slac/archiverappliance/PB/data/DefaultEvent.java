/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.data;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;
import gov.aps.jca.dbr.DBR;
import gov.aps.jca.dbr.DBR_TIME_Byte;
import gov.aps.jca.dbr.DBR_TIME_Double;
import gov.aps.jca.dbr.DBR_TIME_Enum;
import gov.aps.jca.dbr.DBR_TIME_Float;
import gov.aps.jca.dbr.DBR_TIME_Int;
import gov.aps.jca.dbr.DBR_TIME_Short;
import gov.aps.jca.dbr.DBR_TIME_String;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.ByteBufSampleValue;
import org.epics.archiverappliance.data.DBRAlarm;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.data.ScalarStringSampleValue;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.data.VectorStringSampleValue;
import org.epics.archiverappliance.data.VectorValue;
import org.epics.pva.data.PVAByte;
import org.epics.pva.data.PVAByteArray;
import org.epics.pva.data.PVAData;
import org.epics.pva.data.PVADouble;
import org.epics.pva.data.PVADoubleArray;
import org.epics.pva.data.PVAFloat;
import org.epics.pva.data.PVAFloatArray;
import org.epics.pva.data.PVAInt;
import org.epics.pva.data.PVAIntArray;
import org.epics.pva.data.PVAShort;
import org.epics.pva.data.PVAShortArray;
import org.epics.pva.data.PVAString;
import org.epics.pva.data.PVAStringArray;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.PVAStructureArray;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

public record DefaultEvent<T extends SampleValue>(Instant instant, int severity, int status,
                                                  Map<String, String> fieldValues, T value, ArchDBRTypes archDBRTypes) {

    private static final Logger logger = LogManager.getLogger(DefaultEvent.class.getName());


    public static <T extends SampleValue> DefaultEvent<?> fromDBR(ArchDBRTypes archDBRTypes, DBR dbr, Map<String, String> fieldValues) {
        switch (archDBRTypes) {
            case DBR_SCALAR_STRING -> {
                DBR_TIME_String dbrTime = (DBR_TIME_String) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        new ScalarStringSampleValue(dbrTime.getStringValue()[0]),
                        archDBRTypes
                );
            }
            case DBR_SCALAR_SHORT -> {
                DBR_TIME_Short dbrTime = (DBR_TIME_Short) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        new ScalarValue<>(dbrTime.getShortValue()[0]),
                        archDBRTypes
                );
            }
            case DBR_SCALAR_FLOAT -> {
                DBR_TIME_Float dbrTime = (DBR_TIME_Float) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        new ScalarValue<>(dbrTime.getFloatValue()[0]),
                        archDBRTypes
                );
            }
            case DBR_SCALAR_ENUM -> {
                DBR_TIME_Enum dbrTime = (DBR_TIME_Enum) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        new ScalarValue<>(dbrTime.getEnumValue()[0]),
                        archDBRTypes
                );
            }
            case DBR_SCALAR_BYTE -> {
                DBR_TIME_Byte dbrTime = (DBR_TIME_Byte) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        new ScalarValue<>(dbrTime.getByteValue()[0]),
                        archDBRTypes
                );
            }
            case DBR_SCALAR_INT -> {
                DBR_TIME_Int dbrTime = (DBR_TIME_Int) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        new ScalarValue<>(dbrTime.getIntValue()[0]),
                        archDBRTypes
                );
            }
            case DBR_SCALAR_DOUBLE -> {
                DBR_TIME_Double dbrTime = (DBR_TIME_Double) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        new ScalarValue<>(dbrTime.getDoubleValue()[0]),
                        archDBRTypes
                );
            }
            case DBR_WAVEFORM_STRING -> {
                DBR_TIME_String dbrTime = (DBR_TIME_String) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        new VectorStringSampleValue(Arrays.asList(dbrTime.getStringValue())),
                        archDBRTypes
                );
            }
            case DBR_WAVEFORM_SHORT -> {
                DBR_TIME_Short dbrTime = (DBR_TIME_Short) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        new VectorValue<>(Arrays.asList(ArrayUtils.toObject(dbrTime.getShortValue()))),
                        archDBRTypes
                );
            }
            case DBR_WAVEFORM_FLOAT -> {
                DBR_TIME_Float dbrTime = (DBR_TIME_Float) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        new VectorValue<>(Arrays.asList(ArrayUtils.toObject(dbrTime.getFloatValue()))),
                        archDBRTypes
                );
            }
            case DBR_WAVEFORM_ENUM -> {
                DBR_TIME_Enum dbrTime = (DBR_TIME_Enum) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        new VectorValue<>(Arrays.asList(ArrayUtils.toObject(dbrTime.getEnumValue()))),
                        archDBRTypes
                );
            }
            case DBR_WAVEFORM_BYTE -> {
                DBR_TIME_Byte dbrTime = (DBR_TIME_Byte) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        new VectorValue<>(Arrays.asList(ArrayUtils.toObject(dbrTime.getByteValue()))),
                        archDBRTypes
                );
            }
            case DBR_WAVEFORM_INT -> {
                DBR_TIME_Int dbrTime = (DBR_TIME_Int) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        new VectorValue<>(IntStream.of(dbrTime.getIntValue()).boxed().collect(Collectors.toList())),
                        archDBRTypes
                );
            }
            case DBR_WAVEFORM_DOUBLE -> {
                DBR_TIME_Double dbrTime = (DBR_TIME_Double) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        new VectorValue<>(DoubleStream.of(dbrTime.getDoubleValue()).boxed().collect(Collectors.toList())),
                        archDBRTypes
                );
            }
            case DBR_V4_GENERIC_BYTES -> {
                throw new UnsupportedOperationException();
            }
        }
        throw new UnsupportedOperationException();
    }

    public static DefaultEvent<?> fromPVAStructure(ArchDBRTypes archDBRTypes, PVAStructure pvaStructure, Map<String, String> fieldValues) {

        Instant instant = TimeUtils.instantFromPVTimeStamp(pvaStructure.get("timeStamp"));
        DBRAlarm alarm = DBRAlarm.convertPVAlarm(pvaStructure.get("alarm"));
        PVAData value = pvaStructure.get("value");

        switch (archDBRTypes) {
            case DBR_SCALAR_STRING -> {
                return new DefaultEvent<>(
                        instant,
                        alarm.severity(),
                        alarm.status(),
                        fieldValues,
                        new ScalarStringSampleValue(((PVAString) value).get()),
                        archDBRTypes
                );
            }
            case DBR_SCALAR_SHORT -> {
                return new DefaultEvent<>(
                        instant,
                        alarm.severity(),
                        alarm.status(),
                        fieldValues,
                        new ScalarValue<>(((PVAShort) value).get()),
                        archDBRTypes
                );
            }
            case DBR_SCALAR_FLOAT -> {
                return new DefaultEvent<>(
                        instant,
                        alarm.severity(),
                        alarm.status(),
                        fieldValues,
                        new ScalarValue<>(((PVAFloat) value).get()),
                        archDBRTypes
                );
            }
            case DBR_SCALAR_ENUM -> {
                return new DefaultEvent<>(
                        instant,
                        alarm.severity(),
                        alarm.status(),
                        fieldValues,
                        new ScalarValue<>(((PVAInt) ((PVAStructure) value).get("index")).get()),
                        archDBRTypes
                );
            }
            case DBR_SCALAR_BYTE -> {
                return new DefaultEvent<>(
                        instant,
                        alarm.severity(),
                        alarm.status(),
                        fieldValues,
                        new ScalarValue<>(((PVAByte) value).get()),
                        archDBRTypes
                );
            }
            case DBR_SCALAR_INT -> {
                return new DefaultEvent<>(
                        instant,
                        alarm.severity(),
                        alarm.status(),
                        fieldValues,
                        new ScalarValue<>(((PVAInt) value).get()),
                        archDBRTypes
                );
            }
            case DBR_SCALAR_DOUBLE -> {
                return new DefaultEvent<>(
                        instant,
                        alarm.severity(),
                        alarm.status(),
                        fieldValues,
                        new ScalarValue<>(((PVADouble) value).get()),
                        archDBRTypes
                );
            }
            case DBR_WAVEFORM_STRING -> {
                return new DefaultEvent<>(
                        instant,
                        alarm.severity(),
                        alarm.status(),
                        fieldValues,
                        new VectorStringSampleValue(Arrays.asList(((PVAStringArray) value).get())),
                        archDBRTypes
                );
            }
            case DBR_WAVEFORM_SHORT -> {
                return new DefaultEvent<>(
                        instant,
                        alarm.severity(),
                        alarm.status(),
                        fieldValues,
                        new VectorValue<>(Arrays.asList(ArrayUtils.toObject(((PVAShortArray) value).get()))),
                        archDBRTypes
                );
            }
            case DBR_WAVEFORM_FLOAT -> {
                return new DefaultEvent<>(
                        instant,
                        alarm.severity(),
                        alarm.status(),
                        fieldValues,
                        new VectorValue<>(Arrays.asList(ArrayUtils.toObject(((PVAFloatArray) value).get()))),
                        archDBRTypes
                );
            }
            case DBR_WAVEFORM_ENUM -> {

                List<Integer> data = Arrays.stream(((PVAStructureArray) value).get()).map(pvaSArray -> ((PVAInt) pvaSArray.get("index")).get()).toList();

                return new DefaultEvent<>(
                        instant,
                        alarm.severity(),
                        alarm.status(),
                        fieldValues,
                        new VectorValue<>(data),
                        archDBRTypes
                );
            }
            case DBR_WAVEFORM_BYTE -> {
                return new DefaultEvent<>(
                        instant,
                        alarm.severity(),
                        alarm.status(),
                        fieldValues,
                        new VectorValue<>(Arrays.asList(ArrayUtils.toObject(((PVAByteArray) value).get()))),
                        archDBRTypes
                );
            }
            case DBR_WAVEFORM_INT -> {
                return new DefaultEvent<>(
                        instant,
                        alarm.severity(),
                        alarm.status(),
                        fieldValues,
                        new VectorValue<>(IntStream.of(((PVAIntArray) value).get()).boxed().collect(Collectors.toList())),
                        archDBRTypes
                );
            }
            case DBR_WAVEFORM_DOUBLE -> {
                return new DefaultEvent<>(
                        instant,
                        alarm.severity(),
                        alarm.status(),
                        fieldValues,
                        new VectorValue<>(DoubleStream.of(((PVADoubleArray) value).get()).boxed().collect(Collectors.toList())),
                        archDBRTypes
                );
            }
            case DBR_V4_GENERIC_BYTES -> {

                var buffer = ByteBuffer.allocate(4 * 1024 * 1024);
                try {
                    pvaStructure.encodeType(buffer, new BitSet());
                    pvaStructure.encode(buffer);
                    buffer.flip();
                } catch (Exception e) {
                    logger.error("Error serializing pvAccess Generic Bytes ", e);
                }
                return new DefaultEvent<>(
                        instant,
                        alarm.severity(),
                        alarm.status(),
                        fieldValues,
                        new ByteBufSampleValue(buffer),
                        archDBRTypes
                );
            }
        }
        throw new UnsupportedOperationException();
    }

    public static DefaultEvent<?> fromByteArray(ArchDBRTypes archDBRTypes, ByteArray byteArray, short year) {

        try {
            switch (archDBRTypes) {
                case DBR_SCALAR_STRING -> {
                    return fromMessage(archDBRTypes, EPICSEvent.ScalarString.newBuilder()
                            .mergeFrom(byteArray.inPlaceUnescape().unescapedData, byteArray.off, byteArray.unescapedLen)
                            .build(), year);
                }
                case DBR_SCALAR_SHORT -> {
                    return fromMessage(archDBRTypes, EPICSEvent.ScalarShort.newBuilder()
                            .mergeFrom(byteArray.inPlaceUnescape().unescapedData, byteArray.off, byteArray.unescapedLen)
                            .build(), year);
                }
                case DBR_SCALAR_FLOAT -> {
                    return fromMessage(archDBRTypes, EPICSEvent.ScalarFloat.newBuilder()
                            .mergeFrom(byteArray.inPlaceUnescape().unescapedData, byteArray.off, byteArray.unescapedLen)
                            .build(), year);
                }
                case DBR_SCALAR_ENUM -> {
                    return fromMessage(archDBRTypes, EPICSEvent.ScalarEnum.newBuilder()
                            .mergeFrom(byteArray.inPlaceUnescape().unescapedData, byteArray.off, byteArray.unescapedLen)
                            .build(), year);
                }
                case DBR_SCALAR_BYTE -> {
                    return fromMessage(archDBRTypes, EPICSEvent.ScalarByte.newBuilder()
                            .mergeFrom(byteArray.inPlaceUnescape().unescapedData, byteArray.off, byteArray.unescapedLen)
                            .build(), year);
                }
                case DBR_SCALAR_INT -> {
                    return fromMessage(archDBRTypes, EPICSEvent.ScalarInt.newBuilder()
                            .mergeFrom(byteArray.inPlaceUnescape().unescapedData, byteArray.off, byteArray.unescapedLen)
                            .build(), year);
                }
                case DBR_SCALAR_DOUBLE -> {
                    return fromMessage(archDBRTypes, EPICSEvent.ScalarDouble.newBuilder()
                            .mergeFrom(byteArray.inPlaceUnescape().unescapedData, byteArray.off, byteArray.unescapedLen)
                            .build(), year);
                }
                case DBR_WAVEFORM_STRING -> {
                    return fromMessage(archDBRTypes, EPICSEvent.VectorString.newBuilder()
                            .mergeFrom(byteArray.inPlaceUnescape().unescapedData, byteArray.off, byteArray.unescapedLen)
                            .build(), year);
                }
                case DBR_WAVEFORM_SHORT -> {
                    return fromMessage(archDBRTypes, EPICSEvent.VectorShort.newBuilder()
                            .mergeFrom(byteArray.inPlaceUnescape().unescapedData, byteArray.off, byteArray.unescapedLen)
                            .build(), year);
                }
                case DBR_WAVEFORM_FLOAT -> {
                    return fromMessage(archDBRTypes, EPICSEvent.VectorFloat.newBuilder()
                            .mergeFrom(byteArray.inPlaceUnescape().unescapedData, byteArray.off, byteArray.unescapedLen)
                            .build(), year);
                }
                case DBR_WAVEFORM_ENUM -> {
                    return fromMessage(archDBRTypes, EPICSEvent.VectorEnum.newBuilder()
                            .mergeFrom(byteArray.inPlaceUnescape().unescapedData, byteArray.off, byteArray.unescapedLen)
                            .build(), year);
                }
                case DBR_WAVEFORM_BYTE -> {
                    return fromMessage(archDBRTypes, EPICSEvent.VectorChar.newBuilder()
                            .mergeFrom(byteArray.inPlaceUnescape().unescapedData, byteArray.off, byteArray.unescapedLen)
                            .build(), year);
                }
                case DBR_WAVEFORM_INT -> {
                    return fromMessage(archDBRTypes, EPICSEvent.VectorInt.newBuilder()
                            .mergeFrom(byteArray.inPlaceUnescape().unescapedData, byteArray.off, byteArray.unescapedLen)
                            .build(), year);
                }
                case DBR_WAVEFORM_DOUBLE -> {
                    return fromMessage(archDBRTypes, EPICSEvent.VectorDouble.newBuilder()
                            .mergeFrom(byteArray.inPlaceUnescape().unescapedData, byteArray.off, byteArray.unescapedLen)
                            .build(), year);
                }
                case DBR_V4_GENERIC_BYTES -> {
                    return fromMessage(archDBRTypes, EPICSEvent.V4GenericBytes.newBuilder()
                            .mergeFrom(byteArray.inPlaceUnescape().unescapedData, byteArray.off, byteArray.unescapedLen)
                            .build(), year);
                }
            }
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        throw new RuntimeException();
    }

    private static Map<String, String> fieldValuesFromFieldValuesList(List<EPICSEvent.FieldValue> fieldValueList) {
        HashMap<String, String> ret = new HashMap<String, String>();
        if (!fieldValueList.isEmpty()) {
            for (EPICSEvent.FieldValue fieldValue : fieldValueList) {
                ret.put(fieldValue.getName(), fieldValue.getVal());
            }
        }
        return ret;
    }

    public static DefaultEvent<?> fromMessage(ArchDBRTypes archDBRTypes, Message message, short year) {
        switch (archDBRTypes) {
            case DBR_SCALAR_STRING -> {
                var epicsEvent = (EPICSEvent.ScalarString) message;
                YearSecondTimestamp yearSecondTimestamp = new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        new ScalarStringSampleValue(epicsEvent.getVal()),
                        archDBRTypes
                );
            }
            case DBR_SCALAR_SHORT -> {
                var epicsEvent = (EPICSEvent.ScalarShort) message;
                YearSecondTimestamp yearSecondTimestamp = new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        new ScalarValue<>(epicsEvent.getVal()),
                        archDBRTypes
                );
            }
            case DBR_SCALAR_FLOAT -> {
                var epicsEvent = (EPICSEvent.ScalarFloat) message;
                YearSecondTimestamp yearSecondTimestamp = new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        new ScalarValue<>(epicsEvent.getVal()),
                        archDBRTypes
                );
            }
            case DBR_SCALAR_ENUM -> {
                var epicsEvent = (EPICSEvent.ScalarEnum) message;
                YearSecondTimestamp yearSecondTimestamp = new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        new ScalarValue<>((short) epicsEvent.getVal()),
                        archDBRTypes
                );
            }
            case DBR_SCALAR_BYTE -> {
                var epicsEvent = (EPICSEvent.ScalarByte) message;
                YearSecondTimestamp yearSecondTimestamp = new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        new ScalarValue<>(epicsEvent.getVal().byteAt(0)),
                        archDBRTypes
                );
            }
            case DBR_SCALAR_INT -> {
                var epicsEvent = (EPICSEvent.ScalarInt) message;
                YearSecondTimestamp yearSecondTimestamp = new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        new ScalarValue<>(epicsEvent.getVal()),
                        archDBRTypes
                );
            }
            case DBR_SCALAR_DOUBLE -> {
                var epicsEvent = (EPICSEvent.ScalarDouble) message;
                YearSecondTimestamp yearSecondTimestamp = new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        new ScalarValue<>(epicsEvent.getVal()),
                        archDBRTypes
                );
            }
            case DBR_WAVEFORM_STRING -> {
                var epicsEvent = (EPICSEvent.VectorString) message;
                YearSecondTimestamp yearSecondTimestamp = new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        new VectorStringSampleValue(epicsEvent.getValList()),
                        archDBRTypes
                );
            }
            case DBR_WAVEFORM_SHORT -> {
                var epicsEvent = (EPICSEvent.VectorShort) message;
                YearSecondTimestamp yearSecondTimestamp = new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        new VectorValue<>(epicsEvent.getValList()),
                        archDBRTypes
                );
            }
            case DBR_WAVEFORM_FLOAT -> {
                var epicsEvent = (EPICSEvent.VectorFloat) message;
                YearSecondTimestamp yearSecondTimestamp = new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        new VectorValue<>(epicsEvent.getValList()),
                        archDBRTypes
                );
            }
            case DBR_WAVEFORM_ENUM -> {
                var epicsEvent = (EPICSEvent.VectorEnum) message;
                YearSecondTimestamp yearSecondTimestamp = new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        new VectorValue<>(epicsEvent.getValList()),
                        archDBRTypes
                );
            }
            case DBR_WAVEFORM_BYTE -> {
                var epicsEvent = (EPICSEvent.VectorChar) message;
                YearSecondTimestamp yearSecondTimestamp = new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        new VectorValue<>(Arrays.asList(ArrayUtils.toObject(epicsEvent.getVal().toByteArray()))),
                        archDBRTypes
                );
            }
            case DBR_WAVEFORM_INT -> {
                var epicsEvent = (EPICSEvent.VectorInt) message;
                YearSecondTimestamp yearSecondTimestamp = new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        new VectorValue<>(epicsEvent.getValList()),
                        archDBRTypes
                );
            }
            case DBR_WAVEFORM_DOUBLE -> {
                var epicsEvent = (EPICSEvent.VectorDouble) message;
                YearSecondTimestamp yearSecondTimestamp = new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        new VectorValue<>(epicsEvent.getValList()),
                        archDBRTypes
                );
            }
            case DBR_V4_GENERIC_BYTES -> {
                var epicsEvent = (EPICSEvent.V4GenericBytes) message;
                YearSecondTimestamp yearSecondTimestamp = new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        new ByteBufSampleValue(epicsEvent.getVal().asReadOnlyByteBuffer()),
                        archDBRTypes
                );
            }
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DefaultEvent<?> that)) return false;
        return severity == that.severity && status == that.status && Objects.equals(instant, that.instant) && Objects.equals(fieldValues, that.fieldValues) && Objects.equals(value, that.value) && archDBRTypes == that.archDBRTypes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(instant, severity, status, fieldValues, value, archDBRTypes);
    }

    @Override
    public String toString() {
        return "DefaultEvent{" +
                "instant=" + instant +
                ", severity=" + severity +
                ", status=" + status +
                ", fieldValues=" + fieldValues +
                ", value=" + value +
                ", archDBRTypes=" + archDBRTypes +
                '}';
    }

    public ByteArray byteArray() {
        return new ByteArray(LineEscaper.escapeNewLines(message().toByteArray()));
    }

    private List<EPICSEvent.FieldValue> fieldValueList() {
        return this.fieldValues.entrySet().stream().map(e -> EPICSEvent.FieldValue.newBuilder().setName(e.getKey()).setVal(e.getValue()).build()).collect(Collectors.toList());
    }

    public Message message() {
        YearSecondTimestamp yst = TimeUtils.convertToYearSecondTimestamp(instant);
        int secondsintoyear = yst.getSecondsintoyear();
        int nano = yst.getNano();
        switch (this.archDBRTypes) {
            case DBR_SCALAR_STRING -> {
                EPICSEvent.ScalarString.Builder builder = EPICSEvent.ScalarString.newBuilder()
                        .setSecondsintoyear(secondsintoyear)
                        .setNano(nano)
                        .setVal(value.toString());
                if (severity != 0) builder.setSeverity(severity);
                if (status != 0) builder.setStatus(status);
                if (fieldValues != null) {
                    builder.addAllFieldvalues(fieldValueList());
                    builder.setFieldactualchange(true);
                }
                return builder.build();
            }
            case DBR_SCALAR_SHORT -> {
                EPICSEvent.ScalarShort.Builder builder = EPICSEvent.ScalarShort.newBuilder()
                        .setSecondsintoyear(secondsintoyear)
                        .setNano(nano)
                        .setVal(value.getValue().intValue());
                if (severity != 0) builder.setSeverity(severity);
                if (status != 0) builder.setStatus(status);
                if (fieldValues != null) {
                    builder.addAllFieldvalues(fieldValueList());
                    builder.setFieldactualchange(true);
                }
                return builder.build();
            }
            case DBR_SCALAR_FLOAT -> {
                EPICSEvent.ScalarFloat.Builder builder = EPICSEvent.ScalarFloat.newBuilder()
                        .setSecondsintoyear(secondsintoyear)
                        .setNano(nano)
                        .setVal(value.getValue().floatValue());
                if (severity != 0) builder.setSeverity(severity);
                if (status != 0) builder.setStatus(status);
                if (fieldValues != null) {
                    builder.addAllFieldvalues(fieldValueList());
                    builder.setFieldactualchange(true);
                }
                return builder.build();
            }
            case DBR_SCALAR_ENUM -> {
                EPICSEvent.ScalarEnum.Builder builder = EPICSEvent.ScalarEnum.newBuilder()
                        .setSecondsintoyear(secondsintoyear)
                        .setNano(nano)
                        .setVal(value.getValue().intValue());
                if (severity != 0) builder.setSeverity(severity);
                if (status != 0) builder.setStatus(status);
                if (fieldValues != null) {
                    builder.addAllFieldvalues(fieldValueList());
                    builder.setFieldactualchange(true);
                }
                return builder.build();
            }
            case DBR_SCALAR_BYTE -> {
                EPICSEvent.ScalarByte.Builder builder = EPICSEvent.ScalarByte.newBuilder()
                        .setSecondsintoyear(secondsintoyear)
                        .setNano(nano)
                        .setVal(ByteString.copyFrom(new byte[]{value.getValue().byteValue()}));
                if (severity != 0) builder.setSeverity(severity);
                if (status != 0) builder.setStatus(status);
                if (fieldValues != null) {
                    builder.addAllFieldvalues(fieldValueList());
                    builder.setFieldactualchange(true);
                }
                return builder.build();
            }
            case DBR_SCALAR_INT -> {
                EPICSEvent.ScalarInt.Builder builder = EPICSEvent.ScalarInt.newBuilder()
                        .setSecondsintoyear(secondsintoyear)
                        .setNano(nano)
                        .setVal(value.getValue().intValue());
                if (severity != 0) builder.setSeverity(severity);
                if (status != 0) builder.setStatus(status);
                if (fieldValues != null) {
                    builder.addAllFieldvalues(fieldValueList());
                    builder.setFieldactualchange(true);
                }
                return builder.build();
            }

            case DBR_SCALAR_DOUBLE -> {
                EPICSEvent.ScalarDouble.Builder builder = EPICSEvent.ScalarDouble.newBuilder()
                        .setSecondsintoyear(secondsintoyear)
                        .setNano(nano)
                        .setVal(value.getValue().doubleValue());
                if (severity != 0) builder.setSeverity(severity);
                if (status != 0) builder.setStatus(status);
                if (fieldValues != null) {
                    builder.addAllFieldvalues(fieldValueList());
                    builder.setFieldactualchange(true);
                }
                return builder.build();
            }
            case DBR_WAVEFORM_STRING -> {
                EPICSEvent.VectorString.Builder builder = EPICSEvent.VectorString.newBuilder()
                        .setSecondsintoyear(secondsintoyear)
                        .setNano(nano)
                        .addAllVal(value.getStringValues());
                if (severity != 0) builder.setSeverity(severity);
                if (status != 0) builder.setStatus(status);
                if (fieldValues != null) {
                    builder.addAllFieldvalues(fieldValueList());
                    builder.setFieldactualchange(true);
                }
                return builder.build();
            }
            case DBR_WAVEFORM_SHORT -> {
                EPICSEvent.VectorShort.Builder builder = EPICSEvent.VectorShort.newBuilder()
                        .setSecondsintoyear(secondsintoyear)
                        .setNano(nano)
                        .addAllVal(value.getNumberValues());
                if (severity != 0) builder.setSeverity(severity);
                if (status != 0) builder.setStatus(status);
                if (fieldValues != null) {
                    builder.addAllFieldvalues(fieldValueList());
                    builder.setFieldactualchange(true);
                }
                return builder.build();
            }
            case DBR_WAVEFORM_FLOAT -> {
                EPICSEvent.VectorFloat.Builder builder = EPICSEvent.VectorFloat.newBuilder()
                        .setSecondsintoyear(secondsintoyear)
                        .setNano(nano)
                        .addAllVal(value.getNumberValues());
                if (severity != 0) builder.setSeverity(severity);
                if (status != 0) builder.setStatus(status);
                if (fieldValues != null) {
                    builder.addAllFieldvalues(fieldValueList());
                    builder.setFieldactualchange(true);
                }
                return builder.build();
            }
            case DBR_WAVEFORM_ENUM -> {
                EPICSEvent.VectorEnum.Builder builder = EPICSEvent.VectorEnum.newBuilder()
                        .setSecondsintoyear(secondsintoyear)
                        .setNano(nano)
                        .addAllVal(value.getNumberValues());
                if (severity != 0) builder.setSeverity(severity);
                if (status != 0) builder.setStatus(status);
                if (fieldValues != null) {
                    builder.addAllFieldvalues(fieldValueList());
                    builder.setFieldactualchange(true);
                }
                return builder.build();
            }
            case DBR_WAVEFORM_BYTE -> {

                byte[] valueBytes = getBytes();
                EPICSEvent.VectorChar.Builder builder = EPICSEvent.VectorChar.newBuilder()
                        .setSecondsintoyear(secondsintoyear)
                        .setNano(nano)
                        .setVal(ByteString.copyFrom(valueBytes));
                if (severity != 0) builder.setSeverity(severity);
                if (status != 0) builder.setStatus(status);
                if (fieldValues != null) {
                    builder.addAllFieldvalues(fieldValueList());
                    builder.setFieldactualchange(true);
                }
                return builder.build();
            }
            case DBR_WAVEFORM_INT -> {
                EPICSEvent.VectorInt.Builder builder = EPICSEvent.VectorInt.newBuilder()
                        .setSecondsintoyear(secondsintoyear)
                        .setNano(nano)
                        .addAllVal(value.getNumberValues());
                if (severity != 0) builder.setSeverity(severity);
                if (status != 0) builder.setStatus(status);
                if (fieldValues != null) {
                    builder.addAllFieldvalues(fieldValueList());
                    builder.setFieldactualchange(true);
                }
                return builder.build();
            }
            case DBR_WAVEFORM_DOUBLE -> {
                EPICSEvent.VectorDouble.Builder builder = EPICSEvent.VectorDouble.newBuilder()
                        .setSecondsintoyear(secondsintoyear)
                        .setNano(nano)
                        .addAllVal(value.getNumberValues());
                if (severity != 0) builder.setSeverity(severity);
                if (status != 0) builder.setStatus(status);
                if (fieldValues != null) {
                    builder.addAllFieldvalues(fieldValueList());
                    builder.setFieldactualchange(true);
                }
                return builder.build();
            }
            case DBR_V4_GENERIC_BYTES -> {
                EPICSEvent.V4GenericBytes.Builder builder = EPICSEvent.V4GenericBytes.newBuilder()
                        .setSecondsintoyear(secondsintoyear)
                        .setNano(nano)
                        .setVal(ByteString.copyFrom(value.getValueAsBytes()));
                if (severity != 0) builder.setSeverity(severity);
                if (status != 0) builder.setStatus(status);
                if (fieldValues != null) {
                    builder.addAllFieldvalues(fieldValueList());
                    builder.setFieldactualchange(true);
                }
                return builder.build();
            }
        }
        throw new UnsupportedOperationException();
    }

    private byte[] getBytes() {
        List<Byte> bytes = value.getNumberValues();
        byte[] values = new byte[bytes.size()];
        int i = 0;
        for (Byte b : bytes) {
            values[i++] = b;
        }
        return values;
    }

}
