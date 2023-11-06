package edu.stanford.slac.archiverappliance.plain.parquet;

import com.google.protobuf.Message;
import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;
import edu.stanford.slac.archiverappliance.plain.FileInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
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
    final InputFile hadoopInputFile;
    ParquetMetadata footer;
    ParquetFileReader fileReader;
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
    boolean fetchedLastEvent = false;
    boolean fetchedFirstEvent = false;

    static FilterCompat.Filter getSecondsFilter(Integer comparable) {
        FilterPredicate predicate = eq(intColumn(SECONDS_COLUMN_NAME), comparable);
        return FilterCompat.get(predicate);
    }

    public static ParquetInfo fetchFileInfo(Path path) {
        try {
            return new ParquetInfo(path);
        } catch (IOException e) {
            logger.error("Exception reading payload info from path " + path, e);
        }
        return null;
    }

    public ParquetInfo(Path pvPath) throws IOException {
        super();
        var hadoopPath = new org.apache.hadoop.fs.Path(pvPath.toUri());
        var config = new Configuration();
        hadoopInputFile = HadoopInputFile.fromPath(hadoopPath, config);
        fileReader = ParquetFileReader.open(hadoopInputFile);
        footer = fileReader.getFooter();
        var metadata = footer.getFileMetaData();
        this.dataYear = Short.parseShort(metadata.getKeyValueMetaData().get(YEAR));
        this.pvName = metadata.getKeyValueMetaData().get(PV_NAME);

        this.archDBRTypes = ArchDBRTypes.valueOf(metadata.getKeyValueMetaData().get(TYPE));

        logger.debug(() -> String.format(
                "read file meta name %s year %s type %s first event %s last event %s",
                pvName,
                dataYear,
                archDBRTypes,
                getFirstEvent().getEventTimeStamp().toString(),
                getLastEvent().getEventTimeStamp()));
    }

    private DBRTimeEvent getEventAtSeconds(InputFile hadoopInputFile, Integer max) throws IOException {
        Message.Builder lastMessageEvent = null;
        try (var reader = ProtoParquetReader.builder(hadoopInputFile)
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

    private void getFirstEvent(InputFile hadoopInputFile) throws IOException {
        try (var reader = ProtoParquetReader.builder(hadoopInputFile).build()) {

            var value = reader.read();

            if (value != null) {
                firstEvent = constructEvent((Message.Builder) value);
            }
        }
        this.fetchedFirstEvent = true;
    }

    private void getLastEvent(InputFile hadoopInputFile, ParquetMetadata footer) throws IOException {
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
        this.lastEvent = getEventAtSeconds(hadoopInputFile, maxIntoYearSeconds);
        this.fetchedLastEvent = true;
    }

    /**
     * @return
     */
    @Override
    public DBRTimeEvent getFirstEvent() {
        if (!fetchedFirstEvent) {
            try {
                getFirstEvent(hadoopInputFile);
            } catch (IOException e) {
                logger.error("Failed to get first event for file {}", hadoopInputFile, e);
            }
        }
        return this.firstEvent;
    }

    /**
     * @return
     */
    @Override
    public DBRTimeEvent getLastEvent() {
        if (!fetchedLastEvent) {
            try {
                getLastEvent(hadoopInputFile, footer);
            } catch (IOException e) {
                logger.error("Failed to get last event for file {}", hadoopInputFile, e);
            }
        }
        return this.lastEvent;
    }

    public CompressionCodecName getCompressionCodecName() {
        return this.footer.getBlocks().get(0).getColumns().get(0).getCodec();
    }
}
