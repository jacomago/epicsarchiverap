package edu.stanford.slac.archiverappliance.plain.parquet;

import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;
import edu.stanford.slac.archiverappliance.PB.data.PartionedTime;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.proto.ProtoParquetReader;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.EmptyEventIterator;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;

import static edu.stanford.slac.archiverappliance.plain.parquet.ParquetInfo.fetchFileInfo;
import static org.apache.parquet.filter2.predicate.FilterApi.*;

public class ParquetBackedPBEventFileStream implements EventStream, ETLParquetFilesStream {
    private static final Logger logger = LogManager.getLogger(ParquetBackedPBEventFileStream.class.getName());
    private final String pvName;
    private final ArchDBRTypes type;
    private final Path path;
    private final Instant startTime;
    private final Instant endTime;
    private ParquetInfo fileInfo;
    private RemotableEventStreamDesc desc;

    public ParquetBackedPBEventFileStream(String pvName, Path path, ArchDBRTypes type) {
        this(pvName, path, type, null, null);
    }

    public ParquetBackedPBEventFileStream(String pvName, Path path, ArchDBRTypes type, ParquetInfo fileInfo) {
        this(pvName, path, type, null, null, fileInfo);
    }

    public ParquetBackedPBEventFileStream(
            String pvName, Path path, ArchDBRTypes type, Instant startTime, Instant endTime) {
        this.pvName = pvName;
        this.path = path;
        this.type = type;
        this.startTime = startTime;
        this.endTime = endTime;

    }

    public ParquetBackedPBEventFileStream(
            String pvName, Path path, ArchDBRTypes type, Instant startTime, Instant endTime, ParquetInfo fileInfo) {
        this.pvName = pvName;
        this.path = path;
        this.type = type;
        this.startTime = startTime;
        this.endTime = endTime;

        this.fileInfo = fileInfo;
    }

    private ParquetInfo getFileInfo() {
        if (fileInfo == null) {
            this.fileInfo = fetchFileInfo(path);
        }
        return this.fileInfo;
    }

    private static TimePeriod trimDates(
            YearSecondTimestamp startYst,
            YearSecondTimestamp endYst,
            YearSecondTimestamp firstEventTime,
            YearSecondTimestamp lastEventTime) {
        // if year start before file year start reset seconds to 0 and year to file year
        YearSecondTimestamp timePeriodStartYst = startYst;
        if (startYst.compareTo(firstEventTime) < 0) {
            timePeriodStartYst = firstEventTime;
        }

        // if end year after file year, set endYst to last timestamp
        YearSecondTimestamp timePeriodEndYst = endYst;
        if (endYst.compareTo(lastEventTime) > 0) {
            timePeriodEndYst = lastEventTime;
        }

        return new TimePeriod(timePeriodStartYst, timePeriodEndYst);
    }

    @Override
    public void close() throws IOException {
        /* Nothing to close */
    }

    private Iterator<Event> getEventIterator(ParquetReader.Builder<Object> builder) throws IOException {
        return new ParquetBackedPBEventIterator(
                builder.build(),
                DBR2PBTypeMapping.getPBClassFor(this.type).getUnmarshallingFromEpicsEventConstructor(),
                this.getDescription().getYear());
    }

    @Override
    public String toString() {
        return "ParquetBackedPBEventStream{" + "pvName='"
                + pvName + '\'' + ", type="
                + type + ", path="
                + path + ", startTime="
                + startTime + ", endTime="
                + endTime + ", desc="
                + desc + ", fileInfo="
                + fileInfo + '}';
    }

    @Override
    public Iterator<Event> iterator() {
        var hadoopPath = new org.apache.hadoop.fs.Path(this.path.toUri());
        var builder = ProtoParquetReader.builder(hadoopPath);
        if (this.startTime != null && this.endTime != null) {
            YearSecondTimestamp startYst = TimeUtils.convertToYearSecondTimestamp(startTime);
            YearSecondTimestamp endYst = TimeUtils.convertToYearSecondTimestamp(endTime);
            // if no overlap in year return empty
            YearSecondTimestamp firstEventTime =
                    ((PartionedTime) this.getFirstEvent()).getYearSecondTimestamp();
            YearSecondTimestamp lastEventTime = ((PartionedTime) getFileInfo().getLastEvent()).getYearSecondTimestamp();
            if (endYst.compareTo(firstEventTime) < 0 || startYst.compareTo(lastEventTime) > 0) {
                return new EmptyEventIterator();
            }

            builder = builder.withFilter(
                    trimDates(startYst, endYst, firstEventTime, lastEventTime).filter());
        }
        try {
            return getEventIterator(builder);
        } catch (IOException ex) {

            logger.error(ex.getMessage(), ex);
            return new EmptyEventIterator();
        }
    }

    @Override
    public RemotableEventStreamDesc getDescription() {
        if (desc == null) {
            desc = new RemotableEventStreamDesc(this.pvName, getFileInfo());
        }

        return desc;
    }

    @Override
    public List<Path> getPaths() {
        return List.of(this.path);
    }

    /**
     * @param context BasicContext
     */
    @Override
    public Event getFirstEvent(BasicContext context) throws IOException {
        return this.getFirstEvent();
    }

    public Event getFirstEvent() {
        return getFileInfo().getFirstEvent();
    }

    private record TimePeriod(YearSecondTimestamp startYst, YearSecondTimestamp endYst) {

        FilterCompat.Filter filter() {

            FilterPredicate predicate = and(
                    // gtEq start
                    or(
                            and(
                                    eq(intColumn(ParquetInfo.SECONDS_COLUMN_NAME), startYst.getSecondsintoyear()),
                                    gtEq(intColumn(ParquetInfo.NANOSECONDS_COLUMN_NAME), startYst.getNano())),
                            gtEq(intColumn(ParquetInfo.SECONDS_COLUMN_NAME), startYst.getSecondsintoyear() + 1)),
                    // ltEq end
                    or(
                            lt(intColumn(ParquetInfo.SECONDS_COLUMN_NAME), endYst.getSecondsintoyear()),
                            and(
                                    eq(intColumn(ParquetInfo.SECONDS_COLUMN_NAME), endYst.getSecondsintoyear()),
                                    ltEq(intColumn(ParquetInfo.NANOSECONDS_COLUMN_NAME), endYst.getNano()))));

            return FilterCompat.get(predicate);
        }
    }
}
