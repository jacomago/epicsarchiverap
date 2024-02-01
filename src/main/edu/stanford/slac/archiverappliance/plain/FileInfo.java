package edu.stanford.slac.archiverappliance.plain;

import edu.stanford.slac.archiverappliance.plain.parquet.ParquetInfo;
import edu.stanford.slac.archiverappliance.plain.pb.PBFileInfo;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;

public abstract class FileInfo {
    protected DBRTimeEvent firstEvent = null;
    protected DBRTimeEvent lastEvent = null;

    public FileInfo() throws IOException {}

    public static FileInfo extensionPath(FileExtension fileExtension, Path path) throws IOException {
        return switch (fileExtension) {
            case PB -> new PBFileInfo(path);
            case PARQUET -> new ParquetInfo(path);
        };
    }

    public abstract String getPVName();

    public abstract short getDataYear();

    @Override
    public String toString() {
        return "FileInfo{" + "firstEvent=" + firstEvent + ", lastEvent=" + lastEvent + '}';
    }

    public abstract ArchDBRTypes getType();

    public abstract DBRTimeEvent getFirstEvent();

    public abstract DBRTimeEvent getLastEvent();

    public Instant getLastEventInstant() {
        return (getLastEvent() != null) ? getLastEvent().getEventTimeStamp() : Instant.EPOCH;
    }

    public Instant getFirstEventInstant() {
        return (getFirstEvent() != null) ? getFirstEvent().getEventTimeStamp() : Instant.EPOCH;
    }
}
