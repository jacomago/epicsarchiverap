package edu.stanford.slac.archiverappliance.plain.parquet;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStreamDesc;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.EmptyEventIterator;
import org.epics.archiverappliance.config.ArchDBRTypes;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

/**
 * An implementation of {@link ETLParquetFilesStream} that reads from a list of files.
 * Creates an iterator that reads from the files in order of the paths list.
 */
public class ParquetFilesStream implements ETLParquetFilesStream {

    private final String pvName;
    private final ArchDBRTypes archDBRType;
    private final List<Path> paths;
    private final EventStreamDesc desc;
    private ParquetInfo firstPathInfo = null;

    public ParquetFilesStream(String pvName, ArchDBRTypes archDBRType, List<Path> paths) {
        this.pvName = pvName;
        this.archDBRType = archDBRType;
        this.paths = paths;
        this.desc = new EventStreamDesc(this.archDBRType, this.pvName);
    }

    @Override
    public EventStreamDesc getDescription() {
        return desc;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public Iterator<Event> iterator() {
        return new EmptyEventIterator();
    }

    /**
     * Return the paths backing the stream.
     * @return the paths
     */
    @Override
    public List<Path> getPaths() {
        return paths;
    }

    private ParquetInfo getFirstPathInfo() {
        if (firstPathInfo == null) {
            firstPathInfo = ParquetInfo.fetchFileInfo(paths.get(0));
        }
        return firstPathInfo;
    }

    /**
     * Return the first event in the stream.
     * @param context BasicContext for the stream
     * @return the first event
     * @throws IOException on error if reading the file fails
     */
    @Override
    public Event getFirstEvent(BasicContext context) throws IOException {
        if (this.paths.isEmpty()) {
            return null;
        }

        return getFirstPathInfo().getFirstEvent();
    }
}
