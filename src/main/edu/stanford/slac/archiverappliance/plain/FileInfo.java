package edu.stanford.slac.archiverappliance.plain;

import edu.stanford.slac.archiverappliance.plain.parquet.ParquetInfo;
import edu.stanford.slac.archiverappliance.plain.pb.PBFileInfo;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;

import java.io.IOException;
import java.nio.file.Path;

public abstract class FileInfo {
    protected DBRTimeEvent firstEvent = null;
    protected DBRTimeEvent lastEvent = null;

    public FileInfo() throws IOException {}

    public abstract String getPVName();

    public abstract short getDataYear();

    @Override
    public String toString() {
        return "FileInfo{" +
                "firstEvent=" + firstEvent +
                ", lastEvent=" + lastEvent +
                '}';
    }

    public abstract ArchDBRTypes getType();

    public DBRTimeEvent getFirstEvent() {
        return this.firstEvent;
    }
    public DBRTimeEvent getLastEvent() {
        return this.lastEvent;
    }

    public long getLastEventEpochSeconds() {
        return (lastEvent != null) ? lastEvent.getEpochSeconds() : 0;
    }

    public long getFirstEventEpochSeconds() {
        return (firstEvent != null) ? firstEvent.getEpochSeconds() : 0;
    }

    public static FileInfo extensionPath(FileExtension fileExtension, Path path) throws IOException {
        return switch (fileExtension) {
            case PB -> new PBFileInfo(path);
            case PARQUET -> new ParquetInfo(path);
        };
    }
}
