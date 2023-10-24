package edu.stanford.slac.archiverappliance.plain.parquet;

import com.google.protobuf.Message;
import edu.stanford.slac.archiverappliance.PB.data.DBR2PBMessageTypeMapping;
import edu.stanford.slac.archiverappliance.plain.AppendDataStateData;
import edu.stanford.slac.archiverappliance.plain.CompressionMode;
import edu.stanford.slac.archiverappliance.plain.FileInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetReader;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.config.PVNameToKeyMapping;
import org.epics.archiverappliance.etl.ETLBulkStream;
import org.epics.archiverappliance.etl.ETLContext;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

public class ParquetAppendDataStateData extends AppendDataStateData {
    private static final Logger logger = LogManager.getLogger(ParquetAppendDataStateData.class.getName());

    private final CompressionCodecName compressionCodecName;

    ParquetWriter writer;

    /**
     * @param partitionGranularity partitionGranularity of the PB plugin.
     * @param rootFolder RootFolder of the PB plugin
     * @param desc Desc for logging purposes
     * @param lastKnownTimestamp This is probably the most important argument here.
     *                           This is the last known timestamp in this storage.
     *                           If null, we assume time(0) for the last known timestamp.
     * @param pv2key PVNameToKeyMapping
     *
     */
    public ParquetAppendDataStateData(
            PartitionGranularity partitionGranularity,
            String rootFolder,
            String desc,
            Instant lastKnownTimestamp,
            CompressionMode compressionMode,
            PVNameToKeyMapping pv2key) {
        super(partitionGranularity, rootFolder, desc, lastKnownTimestamp, pv2key);
        this.compressionCodecName = compressionMode.getParquetCompressionCodec();
    }

    public static ParquetWriter copyKeepingOpen(Path currentPVPath, FileInfo info) throws IOException {
        logger.debug("parquet copyKeepingOpen  pvPath {} fileInfo {} ", currentPVPath, info);

        // move file
        File newTempFile = new File(currentPVPath.getParent().toString(), currentPVPath.getFileName() + "parquetTemp");
        Path checkSumPath = Path.of(String.valueOf(currentPVPath.getParent()), "." + currentPVPath.getFileName() + ".crc");
        if (Files.exists(checkSumPath)) {
            Files.delete(checkSumPath);
        }

        var fileMoved = currentPVPath.toFile().renameTo(newTempFile);
        if (!fileMoved) {
            throw new IOException("Could not make temporary file at" + newTempFile.getAbsolutePath());
        }

        var currentHadoopPath =
                new org.apache.hadoop.fs.Path(newTempFile.toPath().toUri());
        var newHadoopPath = new org.apache.hadoop.fs.Path(currentPVPath.toUri());
        var messageClass = DBR2PBMessageTypeMapping.getMessageClass(info.getType());
        EpicsParquetWriter.Builder<Object> builder = EpicsParquetWriter.builder(newHadoopPath)
                .withMessage(messageClass)
                .withPVName(info.getPVName())
                .withYear(info.getDataYear())
                .withType(info.getType());
        var writer = builder.build();
        try (var reader = ProtoParquetReader.builder(currentHadoopPath).build()) {

            while (true) {
                Message.Builder message = (Message.Builder) reader.read();
                if (message == null) {
                    break;
                }
                writer.write(message.build());
            }
        }
        assert newTempFile.delete();

        // move and delete old file
        return writer;
    }

    /**
     * If we have an existing file, then this loads a PBInfo, validates the PV name and then updates the appendDataState
     * @param pvName The PV name
     * @param currentPVFilePath The PV path
     * @throws IOException &emsp;
     */
    public void updateStateBasedOnExistingFile(String pvName, Path currentPVFilePath) throws IOException {
        logger.trace("parquet updateStateBasedOnExistingFile  pv {} pvPath {} ", pvName, currentPVFilePath);

        FileInfo info = new ParquetInfo(currentPVFilePath);
        if (!info.getPVName().equals(pvName))
            throw new IOException("Trying to append data for " + pvName + " to a file " + currentPVFilePath
                    + " that has data for " + info.getPVName());
        this.previousYear = info.getDataYear();
        if (info.getLastEvent() != null) {
            this.lastKnownTimeStamp = info.getLastEvent().getEventTimeStamp();
        } else {
            logger.error("Cannot determine last known timestamp when updating state for PV " + pvName + " and path "
                    + currentPVFilePath);
        }

        this.writer = copyKeepingOpen(currentPVFilePath, info);
        this.previousFileName = currentPVFilePath.getFileName().toString();
    }

    /**
     * In cases where we create a new file, this method is used to create an empty file and write out an header.
     * @param pvName The PV name
     * @param pvPath The PV path
     * @param stream The Event stream
     * @throws IOException 	&emsp;
     */
    protected void createNewFileAndWriteAHeader(String pvName, Path pvPath, EventStream stream) throws IOException {

        if (Files.exists(pvPath)) {
            if (Files.size(pvPath) == 0) {
                Files.delete(pvPath);
            } else {
                throw new IOException("Trying to write a header into a file that exists " + pvPath.toAbsolutePath());
            }
        }
        logger.debug(desc + ": Writing new Parquet file" + pvPath.toAbsolutePath()
                + " for PV " + pvName
                + " for year " + this.currentEventsYear
                + " of type " + stream.getDescription().getArchDBRType()
                + " of PBPayload "
                + stream.getDescription().getArchDBRType().getPBPayloadType());
        var hadoopPath = new org.apache.hadoop.fs.Path(pvPath.toUri());
        var messageClass =
                DBR2PBMessageTypeMapping.getMessageClass(stream.getDescription().getArchDBRType());
        assert messageClass != null;
        this.writer = EpicsParquetWriter.builder(hadoopPath)
                .withMessage(messageClass)
                .withPVName(pvName)
                .withYear(this.currentEventsYear)
                .withType(stream.getDescription().getArchDBRType())
                .withCompressionCodec(this.compressionCodecName)
                .build();
        this.previousFileName = pvPath.getFileName().toString();
    }
    /**
     *
     * @param context  &emsp;
     * @param pvName The PV name
     * @param stream  &emsp;
     * @param extension   &emsp;
     * @param extensionToCopyFrom &emsp;
     * @return
     * @throws IOException
     */
    @Override
    public int partitionBoundaryAwareAppendData(
            BasicContext context, String pvName, EventStream stream, String extension, String extensionToCopyFrom)
            throws IOException {

        try (stream) {
            int eventsAppended = 0;
            for (Event event : stream) {
                Instant ts = event.getEventTimeStamp();
                if (shouldISkipEventBasedOnTimeStamps(event)) continue;

                Path pvPath = null;
                shouldISwitchPartitions(context, pvName, extension, ts, CompressionMode.NONE);

                if (this.writer == null) {
                    preparePartition(
                            pvName,
                            stream,
                            context,
                            extension,
                            extensionToCopyFrom,
                            ts,
                            pvPath,
                            CompressionMode.NONE);
                }

                // We check for monotonicity in timestamps again as we had some fresh data from an existing file.
                if (shouldISkipEventBasedOnTimeStamps(event)) continue;

                if (event.getMessage() == null) {
                    logger.error("event {} is null", event);
                    throw new IOException();
                }
                writer.write(event.getMessage());

                this.previousYear = this.currentEventsYear;
                this.lastKnownTimeStamp = event.getEventTimeStamp();
                eventsAppended++;
            }
            return eventsAppended;
        } catch (Throwable t) {
            logger.error("Exception appending data for PV " + pvName, t);
            throw new IOException(t);
        } finally {
            this.closeStreams();
        }
    }

    /**
     *
     * @param pvName The PV name
     * @param context The ETL context
     * @param bulkStream The ETL bulk stream
     * @param extension  &emsp;
     * @param extensionToCopyFrom &emsp;
     * @return
     * @throws IOException
     */
    @Override
    public boolean bulkAppend(
            String pvName, ETLContext context, ETLBulkStream bulkStream, String extension, String extensionToCopyFrom)
            throws IOException {
        logger.info(
                "parquet bulkAppend pv {} extension {} extensiontocopyfrom {}", pvName, extension, extensionToCopyFrom);
        Event firstEvent = bulkStream.getFirstEvent(context);
        if (this.shouldISkipEventBasedOnTimeStamps(firstEvent)) {
            logger.error(
                    "The bulk append functionality works only if we the first event fits cleanly in the current stream for pv "
                            + pvName + " for stream "
                            + bulkStream.getDescription().getSource());
            return false;
        }

        Path pvPath = null;
        if (this.writer == null) {
            pvPath = preparePartition(
                    pvName,
                    bulkStream,
                    context,
                    extension,
                    extensionToCopyFrom,
                    firstEvent.getEventTimeStamp(),
                    pvPath,
                    CompressionMode.NONE);
        }

        // The preparePartition should have created the needed file; so we only append
        assert pvPath != null;
        for (Event event : bulkStream) {
            if (event.getMessage() == null) {
                logger.error("event {} is null", event);
                throw new IOException();
            }
            writer.write(event.getMessage());

            this.previousYear = this.currentEventsYear;
            this.lastKnownTimeStamp = event.getEventTimeStamp();
        }

        this.closeStreams();

        return true;
    }

    /**
     *
     */
    @Override
    public void closeStreams() {
        if (this.writer != null) {
            try {
                logger.debug("close stream with last time stamp {}", this.lastKnownTimeStamp);
                this.writer.close();
            } catch (IOException ignored) {

            }
        }
        this.writer = null;
    }
}
