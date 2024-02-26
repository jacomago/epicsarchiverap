/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PlainPB;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.stanford.slac.archiverappliance.PB.data.PBParseException;
import edu.stanford.slac.archiverappliance.PB.utils.LineByteStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.file.Path;
import java.time.Instant;
import java.util.NoSuchElementException;

/**
 * An iterator for a FileBackedPBEventStream.
 * @author mshankar
 *
 */
public class FileBackedPBEventStreamTimeBasedIterator implements FileBackedPBEventStreamIterator {
    private static final Logger logger = LogManager.getLogger(FileBackedPBEventStreamTimeBasedIterator.class.getName());
    private final Instant startTimeEpochSeconds;
    private final Instant endTimeEpochSeconds;
    private final short year;
    private final LineByteStream lbs;
    private final ArchDBRTypes archDBRTypes;
    Events events = new Events();
    /**
     * A flag indicating if the iterator has been fully read.
     */
    private boolean finished = false;

    public FileBackedPBEventStreamTimeBasedIterator(
            Path path, Instant startTime, Instant endTime, short year, ArchDBRTypes type) throws IOException {
        this.startTimeEpochSeconds = startTime;
        this.endTimeEpochSeconds = endTime;
        assert (startTimeEpochSeconds.getEpochSecond() >= 0);
        assert (endTimeEpochSeconds.getEpochSecond() >= 0);
        assert (endTimeEpochSeconds.compareTo(startTimeEpochSeconds) >= 0);
        this.year = year;
        this.archDBRTypes = type;
        lbs = new LineByteStream(path);
        try {
            // This should read the header
            lbs.readLine(events.line1);
            events.readEvents(lbs);
            while (!events.isEmpty() && !events.startFound()) {
                events.popEvent();
                events.readEvents(lbs);
            }
            logger.info("after start search event1 {}", events.event1.instant());
        } catch (Exception ex) {
            logger.error("Exception getting next event from path " + path.toString(), ex);
            events.clear();
        }
    }

    @Override
    public boolean hasNext() {
        if (!events.isEmpty()) return true;
        if (finished) return false;
        try {
            events.readEvents(lbs);
            if (!events.isEmpty()) return true;
            if (finished) return false;
        } catch (Exception ex) {
            logger.error("Exception creating event object", ex);
        }
        return false;
    }

    @Override
    public Event next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more lines");
        }
        return events.popEvent();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public void close() {
        lbs.safeClose();
    }

    private class Events {
        private final ByteArray line1 = new ByteArray(LineByteStream.MAX_LINE_SIZE);
        private Event event1 = null;

        void readEvents(LineByteStream lbs) throws Exception {
            if (event1 == null) {
                boolean done = false;
                while (!done) {
                    try {
                        lbs.readLine(line1);
                        if (!line1.isEmpty()) {
                            event1 = Event.fromByteArray(archDBRTypes, line1, year);
                            Instant event1EpochSeconds = event1.instant();
                            done = true;
                            if (event1EpochSeconds.isAfter(endTimeEpochSeconds)) {
                                event1 = null;
                                line1.reset();
                                finished = true;
                                return;
                            }
                        }
                        done = true;
                    } catch (InvalidProtocolBufferException | PBParseException ex) {
                        logger.error("InvalidProtocolBufferException|PBParseException processing PB event near "
                                + lbs.getCurrentPosition() + "//0");
                    }
                }
            }
        }

        boolean startFound() {
            if (event1 != null) {
                Instant event1EpochSeconds = event1.instant();
                if (event1EpochSeconds.compareTo(startTimeEpochSeconds) >= 0) {
                    logger.debug(
                            "We have reached an event whose start time is greater than the requested start already. Terminating the search.");
                    return true;
                }
                return false;
            }

            return false;
        }

        Event popEvent() {
            Event previousEvent = event1;
            this.clear();

            return previousEvent;
        }

        boolean isEmpty() {
            return event1 == null;
        }

        void clear() {
            event1 = null;
            line1.reset();
        }
    }
}
