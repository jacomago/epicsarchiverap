/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PlainPB;

import edu.stanford.slac.archiverappliance.parquet.ParquetBackedPBEventStream;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.etl.ETLStreamCreator;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;

/**
 * A stream creator that is backed by a single file.
 * @author mshankar
 *
 */
public class FileStreamCreator implements ETLStreamCreator {
    private final String pvName;
    private final Path path;
    private final FileInfo info;

    private final FileExtension fileExtension;

    public FileStreamCreator(String pvName, Path path, FileInfo fileinfo, FileExtension fileExtension) {
        this.pvName = pvName;
        this.path = path;
        this.info = fileinfo;
        this.fileExtension = fileExtension;
    }

    public static EventStream getTimeStream(
            FileExtension fileExtension,
            String pvName,
            Path path,
            ArchDBRTypes dbrType,
            Instant start,
            Instant end,
            boolean skipSearch) throws IOException {

        return switch (fileExtension) {
            case PB -> new FileBackedPBEventStream(pvName, path, dbrType, start, end, skipSearch);

            case PARQUET -> new ParquetBackedPBEventStream(pvName, path, dbrType, start, end);
        };
    }
    public static EventStream getTimeStream(
            FileExtension fileExtension,
            String pvName,
            Path path,
            Instant start,
            Instant end,
            boolean skipSearch, FileInfo fileInfo) throws IOException {

        return switch (fileExtension) {
            case PB -> new FileBackedPBEventStream(pvName, path, fileInfo.getType(), start, end, skipSearch);

            case PARQUET -> new ParquetBackedPBEventStream(pvName, path, fileInfo.getType(), start, end, fileInfo);
        };
    }

    public static EventStream getStream(
            FileExtension fileExtension,
            String pvName,
            Path path,
            ArchDBRTypes dbrType) throws IOException {

        return switch (fileExtension) {
            case PB -> new FileBackedPBEventStream(pvName, path, dbrType);

            case PARQUET -> new ParquetBackedPBEventStream(pvName, path, dbrType);
        };
    }

    public static EventStream getStream(
            FileExtension fileExtension,
            String pvName,
            Path path,
            FileInfo fileInfo) throws IOException {

        return switch (fileExtension) {
            case PB -> new FileBackedPBEventStream(pvName, path, fileInfo.getType());

            case PARQUET -> new ParquetBackedPBEventStream(pvName, path, fileInfo.getType(), fileInfo);
        };
    }

    public static EventStream getTimeStream(
            String pvName,
            Path path,
            ArchDBRTypes dbrType,
            Instant start,
            Instant end,
            boolean skipSearch) throws IOException {

        return new FileBackedPBEventStream(pvName, path, dbrType, start, end, skipSearch);
    }

    public static EventStream getTimeStream(
            String pvName,
            Path path,
            Instant start,
            Instant end,
            boolean skipSearch, ArchDBRTypes archDBRTypes) throws IOException {

        return new FileBackedPBEventStream(pvName, path, archDBRTypes, start, end, skipSearch);
    }

    public static EventStream getStream(
            String pvName,
            Path path,
            ArchDBRTypes dbrType) throws IOException {

        return new FileBackedPBEventStream(pvName, path, dbrType);
    }

    @Override
    public EventStream getStream() throws IOException {
        return switch (fileExtension) {
            case PB -> new FileBackedPBEventStream(pvName, path, info.getType());

            case PARQUET -> new ParquetBackedPBEventStream(pvName, path, info.getType());
        };
    }
}
