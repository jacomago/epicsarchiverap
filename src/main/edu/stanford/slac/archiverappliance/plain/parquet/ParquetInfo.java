package edu.stanford.slac.archiverappliance.plain.parquet;

import com.google.protobuf.Message;
import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;
import edu.stanford.slac.archiverappliance.plain.FileInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.proto.ProtoParquetReader;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;

import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;

public class ParquetInfo extends FileInfo {
    static final String PV_NAME = "pvName";
    static final String YEAR = "year";
    static final String TYPE = "ArchDBRType";
    private static final Logger logger = LogManager.getLogger(ParquetInfo.class);
    static String SECONDS_COLUMN_NAME = "secondsintoyear";
    static String NANOSECONDS_COLUMN_NAME = "nano";
    String pvName;
    short dataYear;
    ArchDBRTypes archDBRTypes;

    @Override
    public String toString() {
        return "ParquetInfo{" +
                "pvName='" + pvName + '\'' +
                ", dataYear=" + dataYear +
                ", archDBRTypes=" + archDBRTypes +
                ", firstEvent=" + firstEvent +
                ", lastEvent=" + lastEvent +
                '}';
    }

    public ParquetInfo(Path pvPath) throws IOException {
        super();
        var hadoopPath = new org.apache.hadoop.fs.Path(pvPath.toUri());
        var config = new Configuration();


        ParquetMetadata footer =
                ParquetFileReader.readFooter(config, hadoopPath, ParquetMetadataConverter.NO_FILTER);
        var metadata = footer.getFileMetaData();
        this.dataYear = Short.parseShort(metadata.getKeyValueMetaData().get(YEAR));
        this.pvName = metadata.getKeyValueMetaData().get(PV_NAME);

        this.archDBRTypes = ArchDBRTypes.valueOf(metadata.getKeyValueMetaData().get(TYPE));

        getLastEvent(hadoopPath, footer);

        try (var reader = ProtoParquetReader.builder(hadoopPath).build()) {

            var value = reader.read();

            if (value != null) {
                firstEvent = constructEvent((Message.Builder) value);
            }
        }
        logger.debug(
                "read file meta name {} year {} type {} first event {} last event {}",
                pvName,
                dataYear,
                archDBRTypes,
                getFirstEvent().getEventTimeStamp(),
                getLastEvent().getEventTimeStamp());
    }

    static FilterCompat.Filter getSecondsFilter(Integer comparable) {
        FilterPredicate predicate = eq(intColumn(SECONDS_COLUMN_NAME), comparable);
        return FilterCompat.get(predicate);
    }

    private void getLastEvent(org.apache.hadoop.fs.Path hadoopPath, ParquetMetadata footer) throws IOException {
        Integer maxIntoYearSeconds = 0;
        for (BlockMetaData blockMetaData : footer.getBlocks()) {
            var secondsColumn = blockMetaData.getColumns().stream()
                    .filter(c -> c.getPath().toDotString().equals(SECONDS_COLUMN_NAME))
                    .findFirst();
            if (secondsColumn.isPresent()
                    && secondsColumn.get().getStatistics().compareMaxToValue(maxIntoYearSeconds) > 0) {
                maxIntoYearSeconds =
                        (Integer) secondsColumn.get().getStatistics().genericGetMax();
            }
        }
        this.lastEvent = getEventAtSeconds(hadoopPath, maxIntoYearSeconds);
    }

    private DBRTimeEvent getEventAtSeconds(org.apache.hadoop.fs.Path hadoopPath, Integer max) throws IOException {
        Message.Builder lastMessageEvent = null;
        try (var reader = ProtoParquetReader.builder(hadoopPath)
                .withFilter(getSecondsFilter(max))
                .build()) {
            while (true) {
                var value = reader.read();
                if (value != null) {
                    lastMessageEvent = (Message.Builder) value;
                } else {
                    break;
                }
            }
        }

        return constructEvent(lastMessageEvent);
    }

    private DBRTimeEvent constructEvent(Message.Builder event) {
        if (event != null) {
            Constructor<? extends DBRTimeEvent> unmarshallingConstructor =
                    DBR2PBTypeMapping.getPBClassFor(this.archDBRTypes).getUnmarshallingFromEpicsEventConstructor();

            try {
                return unmarshallingConstructor.newInstance(this.dataYear, event);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                logger.warn("Failed to get event", e);
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    public String getPVName() {
        return this.pvName;
    }

    public short getDataYear() {
        return this.dataYear;
    }


    @Override
    public ArchDBRTypes getType() {
        return this.archDBRTypes;
    }
}
