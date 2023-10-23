package edu.stanford.slac.archiverappliance.plain.parquet;

import com.google.protobuf.MessageOrBuilder;
import edu.stanford.slac.archiverappliance.plain.pb.FileBackedPBEventStreamIterator;
import org.apache.parquet.hadoop.ParquetReader;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.data.DBRTimeEvent;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.NoSuchElementException;

public class ParquetBackedPBEventIterator implements FileBackedPBEventStreamIterator {

    ParquetBackedPBEventIterator(
            ParquetReader<Object> reader, Constructor<? extends DBRTimeEvent> unmarshallingConstructor, short year) {
        this.reader = reader;
        this.unmarshallingConstructor = unmarshallingConstructor;
        this.year = year;
    }

    ParquetReader<Object> reader;

    /** The current event. */
    private Event cachedEvent;

    private final Constructor<? extends DBRTimeEvent> unmarshallingConstructor;
    private final short year;
    /** A flag indicating if the iterator has been fully read. */
    private boolean finished = false;
    /**
     *
     * @return
     */
    @Override
    public boolean hasNext() {
        if (cachedEvent != null) {
            return true;
        } else if (finished) {
            return false;
        } else {
            try {

                MessageOrBuilder readEvent = (MessageOrBuilder) reader.read();
                if (readEvent == null) {
                    finished = true;
                    return false;
                } else {
                    cachedEvent = unmarshallingConstructor.newInstance(year, readEvent);
                    return true;
                }

            } catch (Exception ioe) {
                close();
                throw new IllegalStateException(ioe.toString());
            }
        }
    }

    /**
     * Closes the underlying <code>Reader</code> quietly.
     * This method is useful if you only want to process the first few
     * lines of a larger file. If you do not close the iterator
     * then the <code>Reader</code> remains open.
     * This method can safely be called multiple times.
     */
    public void close() {
        try {
            reader.close();
        } catch (IOException ignored) {
        }
        finished = true;
        cachedEvent = null;
    }

    /**
     *
     * @return
     */
    @Override
    public Event next() {
        return nextEvent();
    }

    /**
     * Returns the next line in the wrapped <code>Reader</code>.
     *
     * @return the next line from the input
     * @throws NoSuchElementException if there is no line to return
     */
    public Event nextEvent() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more lines");
        }
        Event currentEvent = cachedEvent;
        cachedEvent = null;
        return currentEvent;
    }
}
