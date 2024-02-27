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
import org.epics.archiverappliance.data.DBRTimeEvent;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

public record DefaultEvent<T extends SampleValue>(
        Instant instant,
        int severity,
        int status,
        Map<String, String> fieldValues,
        boolean actualChange,
        T value,
        ArchDBRTypes archDBRTypes)
        implements DBRTimeEvent {

    private static final Logger logger = LogManager.getLogger(DefaultEvent.class.getName());

    public static DefaultEvent<?> fromDBREvent(DBRTimeEvent dbrTimeEvent) {
        return new DefaultEvent<>(
                dbrTimeEvent.getEventTimeStamp(),
                dbrTimeEvent.getSeverity(),
                dbrTimeEvent.getStatus(),
                dbrTimeEvent.getFields(),
                dbrTimeEvent.isActualChange(),
                dbrTimeEvent.getSampleValue(),
                dbrTimeEvent.getDBRType());
    }

    public static <T extends SampleValue> DefaultEvent<?> fromDBR(
            ArchDBRTypes archDBRTypes, DBR dbr, Map<String, String> fieldValues, boolean actualChange) {
        switch (archDBRTypes) {
            case DBR_SCALAR_STRING -> {
                DBR_TIME_String dbrTime = (DBR_TIME_String) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        actualChange,
                        new ScalarStringSampleValue(dbrTime.getStringValue()[0]),
                        archDBRTypes);
            }
            case DBR_SCALAR_SHORT -> {
                DBR_TIME_Short dbrTime = (DBR_TIME_Short) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        actualChange,
                        new ScalarValue<>(dbrTime.getShortValue()[0]),
                        archDBRTypes);
            }
            case DBR_SCALAR_FLOAT -> {
                DBR_TIME_Float dbrTime = (DBR_TIME_Float) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        actualChange,
                        new ScalarValue<>(dbrTime.getFloatValue()[0]),
                        archDBRTypes);
            }
            case DBR_SCALAR_ENUM -> {
                DBR_TIME_Enum dbrTime = (DBR_TIME_Enum) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        actualChange,
                        new ScalarValue<>((int) dbrTime.getEnumValue()[0]),
                        archDBRTypes);
            }
            case DBR_SCALAR_BYTE -> {
                DBR_TIME_Byte dbrTime = (DBR_TIME_Byte) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        actualChange,
                        new ScalarValue<>(dbrTime.getByteValue()[0]),
                        archDBRTypes);
            }
            case DBR_SCALAR_INT -> {
                DBR_TIME_Int dbrTime = (DBR_TIME_Int) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        actualChange,
                        new ScalarValue<>(dbrTime.getIntValue()[0]),
                        archDBRTypes);
            }
            case DBR_SCALAR_DOUBLE -> {
                DBR_TIME_Double dbrTime = (DBR_TIME_Double) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        actualChange,
                        new ScalarValue<>(dbrTime.getDoubleValue()[0]),
                        archDBRTypes);
            }
            case DBR_WAVEFORM_STRING -> {
                DBR_TIME_String dbrTime = (DBR_TIME_String) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        actualChange,
                        new VectorStringSampleValue(Arrays.asList(dbrTime.getStringValue())),
                        archDBRTypes);
            }
            case DBR_WAVEFORM_SHORT -> {
                DBR_TIME_Short dbrTime = (DBR_TIME_Short) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        actualChange,
                        new VectorValue<>(Arrays.asList(ArrayUtils.toObject(dbrTime.getShortValue()))),
                        archDBRTypes);
            }
            case DBR_WAVEFORM_FLOAT -> {
                DBR_TIME_Float dbrTime = (DBR_TIME_Float) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        actualChange,
                        new VectorValue<>(Arrays.asList(ArrayUtils.toObject(dbrTime.getFloatValue()))),
                        archDBRTypes);
            }
            case DBR_WAVEFORM_ENUM -> {
                DBR_TIME_Enum dbrTime = (DBR_TIME_Enum) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        actualChange,
                        new VectorValue<>(Arrays.stream(ArrayUtils.toObject(dbrTime.getEnumValue())).map(Short::intValue).toList()),
                        archDBRTypes);
            }
            case DBR_WAVEFORM_BYTE -> {
                DBR_TIME_Byte dbrTime = (DBR_TIME_Byte) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        actualChange,
                        new VectorValue<>(Arrays.asList(ArrayUtils.toObject(dbrTime.getByteValue()))),
                        archDBRTypes);
            }
            case DBR_WAVEFORM_INT -> {
                DBR_TIME_Int dbrTime = (DBR_TIME_Int) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        actualChange,
                        new VectorValue<>(
                                IntStream.of(dbrTime.getIntValue()).boxed().collect(Collectors.toList())),
                        archDBRTypes);
            }
            case DBR_WAVEFORM_DOUBLE -> {
                DBR_TIME_Double dbrTime = (DBR_TIME_Double) dbr;

                Instant instant = TimeUtils.convertFromJCATimeStamp(dbrTime.getTimeStamp());
                return new DefaultEvent<>(
                        instant,
                        dbrTime.getSeverity().getValue(),
                        dbrTime.getStatus().getValue(),
                        fieldValues,
                        actualChange,
                        new VectorValue<>(DoubleStream.of(dbrTime.getDoubleValue())
                                .boxed()
                                .collect(Collectors.toList())),
                        archDBRTypes);
            }
            case DBR_V4_GENERIC_BYTES -> {
                throw new UnsupportedOperationException();
            }
        }
        throw new UnsupportedOperationException();
    }

    public static DefaultEvent<?> fromPVAStructure(
            ArchDBRTypes archDBRTypes,
            PVAStructure pvaStructure,
            Map<String, String> fieldValues,
            boolean actualChange) {

        Instant instant = TimeUtils.instantFromPVTimeStamp(pvaStructure.get("timeStamp"));
        DBRAlarm alarm = DBRAlarm.convertPVAlarm(pvaStructure.get("alarm"));
        PVAData value = pvaStructure.get("value");

        SampleValue sampleValue =
                switch (archDBRTypes) {
                    case DBR_SCALAR_STRING -> new ScalarStringSampleValue(((PVAString) value).get());

                    case DBR_SCALAR_SHORT -> new ScalarValue<>(((PVAShort) value).get());

                    case DBR_SCALAR_FLOAT -> new ScalarValue<>(((PVAFloat) value).get());

                    case DBR_SCALAR_ENUM -> new ScalarValue<>(
                            ((PVAInt) ((PVAStructure) value).get("index")).get());

                    case DBR_SCALAR_BYTE -> new ScalarValue<>(((PVAByte) value).get());

                    case DBR_SCALAR_INT -> new ScalarValue<>(((PVAInt) value).get());

                    case DBR_SCALAR_DOUBLE -> new ScalarValue<>(((PVADouble) value).get());

                    case DBR_WAVEFORM_STRING -> new VectorStringSampleValue(
                            Arrays.asList(((PVAStringArray) value).get()));
                    case DBR_WAVEFORM_SHORT -> new VectorValue<>(
                            Arrays.stream(ArrayUtils.toObject(((PVAShortArray) value).get())).map(Short::intValue).toList());
                    case DBR_WAVEFORM_FLOAT -> new VectorValue<>(
                            Arrays.asList(ArrayUtils.toObject(((PVAFloatArray) value).get())));
                    case DBR_WAVEFORM_ENUM -> new VectorValue<>(Arrays.stream(((PVAStructureArray) value).get())
                            .map(pvaSArray -> (((PVAInt) pvaSArray.get("index")).get()))
                            .toList());

                    case DBR_WAVEFORM_BYTE -> new VectorValue<>(
                            Arrays.asList(ArrayUtils.toObject(((PVAByteArray) value).get())));

                    case DBR_WAVEFORM_INT -> new VectorValue<>(
                            IntStream.of(((PVAIntArray) value).get()).boxed().collect(Collectors.toList()));
                    case DBR_WAVEFORM_DOUBLE -> new VectorValue<>(DoubleStream.of(((PVADoubleArray) value).get())
                            .boxed()
                            .collect(Collectors.toList()));
                    case DBR_V4_GENERIC_BYTES -> {
                        var buffer = ByteBuffer.allocate(4 * 1024 * 1024);
                        try {
                            pvaStructure.encodeType(buffer, new BitSet());
                            pvaStructure.encode(buffer);
                            buffer.flip();
                        } catch (Exception e) {
                            logger.error("Error serializing pvAccess Generic Bytes ", e);
                        }
                        yield new ByteBufSampleValue(buffer);
                    }
                };
        return new DefaultEvent<>(
                instant, alarm.severity(), alarm.status(), fieldValues, actualChange, sampleValue, archDBRTypes);
    }

    public static DefaultEvent<?> fromByteArray(ArchDBRTypes archDBRTypes, short year, ByteArray byteArray) {

        try {
            switch (archDBRTypes) {
                case DBR_SCALAR_STRING -> {
                    return fromMessage(
                            archDBRTypes,
                            EPICSEvent.ScalarString.newBuilder()
                                    .mergeFrom(
                                            byteArray.inPlaceUnescape().unescapedData,
                                            byteArray.off,
                                            byteArray.unescapedLen)
                                    .build(),
                            year);
                }
                case DBR_SCALAR_SHORT -> {
                    return fromMessage(
                            archDBRTypes,
                            EPICSEvent.ScalarShort.newBuilder()
                                    .mergeFrom(
                                            byteArray.inPlaceUnescape().unescapedData,
                                            byteArray.off,
                                            byteArray.unescapedLen)
                                    .build(),
                            year);
                }
                case DBR_SCALAR_FLOAT -> {
                    return fromMessage(
                            archDBRTypes,
                            EPICSEvent.ScalarFloat.newBuilder()
                                    .mergeFrom(
                                            byteArray.inPlaceUnescape().unescapedData,
                                            byteArray.off,
                                            byteArray.unescapedLen)
                                    .build(),
                            year);
                }
                case DBR_SCALAR_ENUM -> {
                    return fromMessage(
                            archDBRTypes,
                            EPICSEvent.ScalarEnum.newBuilder()
                                    .mergeFrom(
                                            byteArray.inPlaceUnescape().unescapedData,
                                            byteArray.off,
                                            byteArray.unescapedLen)
                                    .build(),
                            year);
                }
                case DBR_SCALAR_BYTE -> {
                    return fromMessage(
                            archDBRTypes,
                            EPICSEvent.ScalarByte.newBuilder()
                                    .mergeFrom(
                                            byteArray.inPlaceUnescape().unescapedData,
                                            byteArray.off,
                                            byteArray.unescapedLen)
                                    .build(),
                            year);
                }
                case DBR_SCALAR_INT -> {
                    return fromMessage(
                            archDBRTypes,
                            EPICSEvent.ScalarInt.newBuilder()
                                    .mergeFrom(
                                            byteArray.inPlaceUnescape().unescapedData,
                                            byteArray.off,
                                            byteArray.unescapedLen)
                                    .build(),
                            year);
                }
                case DBR_SCALAR_DOUBLE -> {
                    return fromMessage(
                            archDBRTypes,
                            EPICSEvent.ScalarDouble.newBuilder()
                                    .mergeFrom(
                                            byteArray.inPlaceUnescape().unescapedData,
                                            byteArray.off,
                                            byteArray.unescapedLen)
                                    .build(),
                            year);
                }
                case DBR_WAVEFORM_STRING -> {
                    return fromMessage(
                            archDBRTypes,
                            EPICSEvent.VectorString.newBuilder()
                                    .mergeFrom(
                                            byteArray.inPlaceUnescape().unescapedData,
                                            byteArray.off,
                                            byteArray.unescapedLen)
                                    .build(),
                            year);
                }
                case DBR_WAVEFORM_SHORT -> {
                    return fromMessage(
                            archDBRTypes,
                            EPICSEvent.VectorShort.newBuilder()
                                    .mergeFrom(
                                            byteArray.inPlaceUnescape().unescapedData,
                                            byteArray.off,
                                            byteArray.unescapedLen)
                                    .build(),
                            year);
                }
                case DBR_WAVEFORM_FLOAT -> {
                    return fromMessage(
                            archDBRTypes,
                            EPICSEvent.VectorFloat.newBuilder()
                                    .mergeFrom(
                                            byteArray.inPlaceUnescape().unescapedData,
                                            byteArray.off,
                                            byteArray.unescapedLen)
                                    .build(),
                            year);
                }
                case DBR_WAVEFORM_ENUM -> {
                    return fromMessage(
                            archDBRTypes,
                            EPICSEvent.VectorEnum.newBuilder()
                                    .mergeFrom(
                                            byteArray.inPlaceUnescape().unescapedData,
                                            byteArray.off,
                                            byteArray.unescapedLen)
                                    .build(),
                            year);
                }
                case DBR_WAVEFORM_BYTE -> {
                    return fromMessage(
                            archDBRTypes,
                            EPICSEvent.VectorChar.newBuilder()
                                    .mergeFrom(
                                            byteArray.inPlaceUnescape().unescapedData,
                                            byteArray.off,
                                            byteArray.unescapedLen)
                                    .build(),
                            year);
                }
                case DBR_WAVEFORM_INT -> {
                    return fromMessage(
                            archDBRTypes,
                            EPICSEvent.VectorInt.newBuilder()
                                    .mergeFrom(
                                            byteArray.inPlaceUnescape().unescapedData,
                                            byteArray.off,
                                            byteArray.unescapedLen)
                                    .build(),
                            year);
                }
                case DBR_WAVEFORM_DOUBLE -> {
                    return fromMessage(
                            archDBRTypes,
                            EPICSEvent.VectorDouble.newBuilder()
                                    .mergeFrom(
                                            byteArray.inPlaceUnescape().unescapedData,
                                            byteArray.off,
                                            byteArray.unescapedLen)
                                    .build(),
                            year);
                }
                case DBR_V4_GENERIC_BYTES -> {
                    return fromMessage(
                            archDBRTypes,
                            EPICSEvent.V4GenericBytes.newBuilder()
                                    .mergeFrom(
                                            byteArray.inPlaceUnescape().unescapedData,
                                            byteArray.off,
                                            byteArray.unescapedLen)
                                    .build(),
                            year);
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
                YearSecondTimestamp yearSecondTimestamp =
                        new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        epicsEvent.getFieldactualchange(),
                        new ScalarStringSampleValue(epicsEvent.getVal()),
                        archDBRTypes);
            }
            case DBR_SCALAR_SHORT -> {
                var epicsEvent = (EPICSEvent.ScalarShort) message;
                YearSecondTimestamp yearSecondTimestamp =
                        new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        epicsEvent.getFieldactualchange(),
                        new ScalarValue<>(epicsEvent.getVal()),
                        archDBRTypes);
            }
            case DBR_SCALAR_FLOAT -> {
                var epicsEvent = (EPICSEvent.ScalarFloat) message;
                YearSecondTimestamp yearSecondTimestamp =
                        new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        epicsEvent.getFieldactualchange(),
                        new ScalarValue<>(epicsEvent.getVal()),
                        archDBRTypes);
            }
            case DBR_SCALAR_ENUM -> {
                var epicsEvent = (EPICSEvent.ScalarEnum) message;
                YearSecondTimestamp yearSecondTimestamp =
                        new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        epicsEvent.getFieldactualchange(),
                        new ScalarValue<>(epicsEvent.getVal()),
                        archDBRTypes);
            }
            case DBR_SCALAR_BYTE -> {
                var epicsEvent = (EPICSEvent.ScalarByte) message;
                YearSecondTimestamp yearSecondTimestamp =
                        new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        epicsEvent.getFieldactualchange(),
                        new ScalarValue<>(epicsEvent.getVal().byteAt(0)),
                        archDBRTypes);
            }
            case DBR_SCALAR_INT -> {
                var epicsEvent = (EPICSEvent.ScalarInt) message;
                YearSecondTimestamp yearSecondTimestamp =
                        new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        epicsEvent.getFieldactualchange(),
                        new ScalarValue<>(epicsEvent.getVal()),
                        archDBRTypes);
            }
            case DBR_SCALAR_DOUBLE -> {
                var epicsEvent = (EPICSEvent.ScalarDouble) message;
                YearSecondTimestamp yearSecondTimestamp =
                        new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        epicsEvent.getFieldactualchange(),
                        new ScalarValue<>(epicsEvent.getVal()),
                        archDBRTypes);
            }
            case DBR_WAVEFORM_STRING -> {
                var epicsEvent = (EPICSEvent.VectorString) message;
                YearSecondTimestamp yearSecondTimestamp =
                        new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        epicsEvent.getFieldactualchange(),
                        new VectorStringSampleValue(epicsEvent.getValList()),
                        archDBRTypes);
            }
            case DBR_WAVEFORM_SHORT -> {
                var epicsEvent = (EPICSEvent.VectorShort) message;
                YearSecondTimestamp yearSecondTimestamp =
                        new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        epicsEvent.getFieldactualchange(),
                        new VectorValue<>(epicsEvent.getValList()),
                        archDBRTypes);
            }
            case DBR_WAVEFORM_FLOAT -> {
                var epicsEvent = (EPICSEvent.VectorFloat) message;
                YearSecondTimestamp yearSecondTimestamp =
                        new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        epicsEvent.getFieldactualchange(),
                        new VectorValue<>(epicsEvent.getValList()),
                        archDBRTypes);
            }
            case DBR_WAVEFORM_ENUM -> {
                var epicsEvent = (EPICSEvent.VectorEnum) message;
                YearSecondTimestamp yearSecondTimestamp =
                        new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        epicsEvent.getFieldactualchange(),
                        new VectorValue<>(epicsEvent.getValList()),
                        archDBRTypes);
            }
            case DBR_WAVEFORM_BYTE -> {
                var epicsEvent = (EPICSEvent.VectorChar) message;
                YearSecondTimestamp yearSecondTimestamp =
                        new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        epicsEvent.getFieldactualchange(),
                        new VectorValue<>(Arrays.asList(
                                ArrayUtils.toObject(epicsEvent.getVal().toByteArray()))),
                        archDBRTypes);
            }
            case DBR_WAVEFORM_INT -> {
                var epicsEvent = (EPICSEvent.VectorInt) message;
                YearSecondTimestamp yearSecondTimestamp =
                        new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        epicsEvent.getFieldactualchange(),
                        new VectorValue<>(epicsEvent.getValList()),
                        archDBRTypes);
            }
            case DBR_WAVEFORM_DOUBLE -> {
                var epicsEvent = (EPICSEvent.VectorDouble) message;
                YearSecondTimestamp yearSecondTimestamp =
                        new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        epicsEvent.getFieldactualchange(),
                        new VectorValue<>(epicsEvent.getValList()),
                        archDBRTypes);
            }
            case DBR_V4_GENERIC_BYTES -> {
                var epicsEvent = (EPICSEvent.V4GenericBytes) message;
                YearSecondTimestamp yearSecondTimestamp =
                        new YearSecondTimestamp(year, epicsEvent.getSecondsintoyear(), epicsEvent.getNano());
                return new DefaultEvent<>(
                        TimeUtils.convertFromYearSecondTimestamp(yearSecondTimestamp),
                        epicsEvent.getSeverity(),
                        epicsEvent.getStatus(),
                        fieldValuesFromFieldValuesList(epicsEvent.getFieldvaluesList()),
                        epicsEvent.getFieldactualchange(),
                        new ByteBufSampleValue(epicsEvent.getVal().asReadOnlyByteBuffer()),
                        archDBRTypes);
            }
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DefaultEvent<?> that)) return false;
        return getSeverity() == that.getSeverity() && getStatus() == that.getStatus() && isActualChange() == that.isActualChange() && Objects.equals(instant, that.instant) && Objects.equals(fieldValues, that.fieldValues) && Objects.equals(value, that.value) && archDBRTypes == that.archDBRTypes;
    }

    @Override
    public String toString() {
        return "DefaultEvent{" +
                "instant=" + instant +
                ", severity=" + severity +
                ", status=" + status +
                ", fieldValues=" + fieldValues +
                ", actualChange=" + actualChange +
                ", value=" + value +
                ", archDBRTypes=" + archDBRTypes +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(instant, getSeverity(), getStatus(), fieldValues, isActualChange(), value, archDBRTypes);
    }

    public ByteArray byteArray() {
        return new ByteArray(LineEscaper.escapeNewLines(message().toByteArray()));
    }

    private List<EPICSEvent.FieldValue> fieldValueList() {
        return this.fieldValues.entrySet().stream()
                .map(e -> EPICSEvent.FieldValue.newBuilder()
                        .setName(e.getKey())
                        .setVal(e.getValue())
                        .build())
                .collect(Collectors.toList());
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
                    builder.setFieldactualchange(actualChange);
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
                    builder.setFieldactualchange(actualChange);
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
                    builder.setFieldactualchange(actualChange);
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
                    builder.setFieldactualchange(actualChange);
                }
                return builder.build();
            }
            case DBR_SCALAR_BYTE -> {
                EPICSEvent.ScalarByte.Builder builder = EPICSEvent.ScalarByte.newBuilder()
                        .setSecondsintoyear(secondsintoyear)
                        .setNano(nano)
                        .setVal(ByteString.copyFrom(new byte[] {value.getValue().byteValue()}));
                if (severity != 0) builder.setSeverity(severity);
                if (status != 0) builder.setStatus(status);
                if (fieldValues != null) {
                    builder.addAllFieldvalues(fieldValueList());
                    builder.setFieldactualchange(actualChange);
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
                    builder.setFieldactualchange(actualChange);
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
                    builder.setFieldactualchange(actualChange);
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
                    builder.setFieldactualchange(actualChange);
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
                    builder.setFieldactualchange(actualChange);
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
                    builder.setFieldactualchange(actualChange);
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
                    builder.setFieldactualchange(actualChange);
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
                    builder.setFieldactualchange(actualChange);
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
                    builder.setFieldactualchange(actualChange);
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
                    builder.setFieldactualchange(actualChange);
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
                    builder.setFieldactualchange(actualChange);
                }
                return builder.build();
            }
        }
        throw new UnsupportedOperationException();
    }

    public static DefaultEvent<?> fromCSV(String[] line, ArchDBRTypes archDBRTypes) throws Exception {
        // This line is of this format epochseconds, nanos, value, status, severity
        // Example: 1301986801,446452000,5.55269,0,0
        // Waveforms are pipe escaped....
        if (line == null || line.length < 5)
            throw new Exception(
                    "We need at least five columns in the CSV - epochseconds, nanos, value, status, severity. Example: - 1301986801,446452000,5.55269,0,0");
        // Per Bob, the epochseconds here is EPICS epoch seconds. We need to convert to Java epoch seconds; so we add
        // the offset.
        long epochSeconds = Long.parseLong(line[0]) + TimeUtils.EPICS_EPOCH_2_JAVA_EPOCH_OFFSET;
        int nanos = Integer.parseInt(line[1]);
        Instant timestamp = TimeUtils.convertFromEpochSeconds(epochSeconds, nanos);
        String valueStr = line[2];
        String[] vectorValueStr = valueStr.split("\\|");
        int status = Integer.parseInt(line[3]);
        int severity = Integer.parseInt(line[4]);
        SampleValue sampleValue;
        switch (archDBRTypes) {
            case DBR_SCALAR_STRING, DBR_V4_GENERIC_BYTES:
                sampleValue = new ScalarStringSampleValue(valueStr);
                break;
            case DBR_SCALAR_SHORT, DBR_SCALAR_ENUM, DBR_SCALAR_INT:
                sampleValue = new ScalarValue<>(Integer.valueOf(valueStr));
                break;
            case DBR_SCALAR_FLOAT:
                sampleValue = new ScalarValue<>(Float.valueOf(valueStr));
                break;
            case DBR_SCALAR_BYTE:
                sampleValue = new ScalarValue<>(Byte.valueOf(valueStr));
                break;
            case DBR_SCALAR_DOUBLE:
                sampleValue = new ScalarValue<>(Double.valueOf(valueStr));
                break;
            case DBR_WAVEFORM_STRING:
                if (valueStr.isEmpty()) {
                    sampleValue = new VectorStringSampleValue(List.of());
                } else {
                    sampleValue = new VectorStringSampleValue(Arrays.asList(vectorValueStr));
                }
                break;
            case DBR_WAVEFORM_SHORT, DBR_WAVEFORM_ENUM, DBR_WAVEFORM_INT:
                {
                    if (valueStr.isEmpty()) {
                        sampleValue = new VectorValue<Integer>(List.of());
                    } else {
                        ArrayList<Integer> vals = new ArrayList<>(vectorValueStr.length);
                        for (String val : vectorValueStr) {
                            vals.add(Integer.valueOf(val));
                        }
                        sampleValue = new VectorValue<>(vals);
                    }
                }
                break;
            case DBR_WAVEFORM_FLOAT:
                {
                    if (valueStr.isEmpty()) {
                        sampleValue = new VectorValue<Float>(List.of());
                    } else {
                        ArrayList<Float> vals = new ArrayList<>(vectorValueStr.length);
                        for (String val : vectorValueStr) {
                            vals.add(Float.valueOf(val));
                        }
                        sampleValue = new VectorValue<>(vals);
                    }
                }
                break;
            case DBR_WAVEFORM_BYTE:
                {
                    if (valueStr.isEmpty()) {
                        sampleValue = new VectorValue<Byte>(List.of());
                    } else {
                        ArrayList<Byte> vals = new ArrayList<>(vectorValueStr.length);
                        for (String val : vectorValueStr) {
                            vals.add(Byte.valueOf(val));
                        }
                        sampleValue = new VectorValue<>(vals);
                    }
                }
                break;
            case DBR_WAVEFORM_DOUBLE:
                {
                    if (valueStr.isEmpty()) {
                        sampleValue = new VectorValue<Double>(List.of());
                    } else {
                        ArrayList<Double> vals = new ArrayList<Double>(vectorValueStr.length);
                        for (String val : vectorValueStr) {
                            vals.add(Double.valueOf(val));
                        }
                        sampleValue = new VectorValue<Double>(vals);
                    }
                }
                break;

            default:
                throw new Exception("Unsupported DBR type in swicth statement " + archDBRTypes.toString());
        }
        return new DefaultEvent<>(timestamp, severity, status, Map.of(), false, sampleValue, archDBRTypes);
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

    @Override
    public long getEpochSeconds() {
        return instant.getEpochSecond();
    }

    @Override
    public Instant getEventTimeStamp() {
        return instant;
    }

    @Override
    public ByteArray getRawForm() {
        return byteArray();
    }

    @Override
    public SampleValue getSampleValue() {
        return value();
    }

    @Override
    public ArchDBRTypes getDBRType() {
        return archDBRTypes;
    }

    @Override
    public int getStatus() {
        return status;
    }

    @Override
    public int getSeverity() {
        return severity;
    }

    @Override
    public boolean hasFieldValues() {
        return !this.fieldValues.isEmpty();
    }

    @Override
    public boolean isActualChange() {
        return actualChange;
    }

    @Override
    public Map<String, String> getFields() {
        return fieldValues;
    }

    @Override
    public String getFieldValue(String fieldName) {
        return fieldValues.get(fieldName);
    }

    @Override
    public int getRepeatCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public DBRTimeEvent cloneWithExtraFieldValues(Map<String, String> fieldValues, boolean actualChange) {
        var currentFieldValues = new HashMap<>(this.fieldValues);
        currentFieldValues.putAll(fieldValues);
        return new DefaultEvent<SampleValue>(
                this.instant,
                this.severity,
                this.status,
                currentFieldValues,
                actualChange,
                this.value,
                this.archDBRTypes);
    }

    public DBRTimeEvent cloneWithValue(SampleValue value, ArchDBRTypes archDBRTypes) {
        return new DefaultEvent<>(
                this.instant, this.severity, this.status, this.fieldValues, this.actualChange, value, archDBRTypes);
    }
}
