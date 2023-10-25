package edu.stanford.slac.archiverappliance.plain.parquet;

import edu.stanford.slac.archiverappliance.PB.data.DBR2PBMessageTypeMapping;
import edu.stanford.slac.archiverappliance.plain.AppendDataStateData;
import edu.stanford.slac.archiverappliance.plain.CompressionMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.rewrite.ParquetRewriter;
import org.apache.parquet.hadoop.rewrite.RewriteOptions;
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
import java.util.ArrayDeque;

public class ParquetAppendDataStateData extends AppendDataStateData {
    private static final Logger logger = LogManager.getLogger(ParquetAppendDataStateData.class.getName());

    private final ArrayDeque<Path> tempFiles;
    private static final String tempFileFormatString = "%sTemp%d";
    ParquetWriter<Object> writer;
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
        super(partitionGranularity, rootFolder, desc, lastKnownTimestamp, pv2key, compressionMode);
        this.tempFiles = new ArrayDeque<>();
    }

    public ParquetWriter<Object> buildWriterExistingFile(Path currentPVPath, ParquetInfo info) throws IOException {
        logger.debug("parquet buildWriterExistingFile  pvPath {} fileInfo {} ", currentPVPath, info);

        File newTempFile = moveCurrentFile(currentPVPath);

        new org.apache.hadoop.fs.Path(newTempFile.toPath().toUri());
        var newHadoopPath = new org.apache.hadoop.fs.Path(currentPVPath.toUri());
        var messageClass = DBR2PBMessageTypeMapping.getMessageClass(info.getType());
        EpicsParquetWriter.Builder<Object> builder = EpicsParquetWriter.builder(newHadoopPath)
                .withMessage(messageClass)
                .withPVName(info.getPVName())
                .withYear(info.getDataYear())
                .withType(info.getType());

        return builder.build();
    }

    private File moveCurrentFile(Path currentPVPath) throws IOException {
        // move file
        File newTempFile = new File(
                currentPVPath.getParent().toString(), String.format(tempFileFormatString, currentPVPath.getFileName(), this.tempFiles.size()));
        Path checkSumPath =
                Path.of(String.valueOf(currentPVPath.getParent()), "." + currentPVPath.getFileName() + ".crc");
        if (Files.exists(checkSumPath)) {
            Files.delete(checkSumPath);
        }

        var fileMoved = currentPVPath.toFile().renameTo(newTempFile);
        tempFiles.add(newTempFile.toPath());
        if (!fileMoved) {
            throw new IOException("Could not make temporary file at" + newTempFile.getAbsolutePath());
        }
        return newTempFile;
    }

    /**
     * If we have an existing file, then this loads a PBInfo, validates the PV name and then updates the appendDataState
     * @param pvName The PV name
     * @param currentPVFilePath The PV path
     * @throws IOException &emsp;
     */
    public void updateStateBasedOnExistingFile(String pvName, Path currentPVFilePath) throws IOException {
        logger.debug("parquet updateStateBasedOnExistingFile  pv {} pvPath {} ", pvName, currentPVFilePath);

        ParquetInfo info = new ParquetInfo(currentPVFilePath);
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

        this.writer = buildWriterExistingFile(currentPVFilePath, info);
        this.previousFilePath = currentPVFilePath;
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
                .withCompressionCodec(this.compressionMode.getParquetCompressionCodec())
                .build();
        this.previousFilePath = pvPath;
    }
    /**
     *
     * @param context  &emsp;
     * @param pvName The PV name
     * @param stream  &emsp;
     * @param extension   &emsp;
     * @param extensionToCopyFrom &emsp;
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

                shouldISwitchPartitions(context, pvName, extension, ts, this.compressionMode);

                if (this.writer == null) {
                    preparePartition(
                            pvName, stream, context, extension, extensionToCopyFrom, ts, null, this.compressionMode);
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
     */
    @Override
    public boolean bulkAppend(
            String pvName, ETLContext context, ETLBulkStream bulkStream, String extension, String extensionToCopyFrom)
            throws IOException {
        Event firstEvent = checkStream(pvName, context, bulkStream, ETLParquetFilesStream.class);
        if (firstEvent == null) return false;
        ETLParquetFilesStream etlParquetFilesStream = (ETLParquetFilesStream) bulkStream;

        Path pvPath = null;
        if (this.writer == null) {
            pvPath = preparePartition(
                    pvName,
                    bulkStream,
                    context,
                    extension,
                    extensionToCopyFrom,
                    firstEvent.getEventTimeStamp(),
                    null,
                    this.compressionMode);
        }

        // The preparePartition should have created the needed file; so we only append
        assert pvPath != null;
        tempFiles.add(etlParquetFilesStream.getPath());
        this.closeStreams();
        try {
            // Update the last known timestamp and the like...
            updateStateBasedOnExistingFile(pvName, pvPath);
        } finally {
            this.closeStreams();
        }
        return true;
    }

    @Override
    public void closeStreams() throws IOException {
        if (this.writer != null) {
            try {
                logger.debug("close stream with last time stamp {}", this.lastKnownTimeStamp);
                this.writer.close();
            } catch (IOException ignored) {

            }
        }
        combineWithTempFiles();
        this.writer = null;
    }

    private void combineWithTempFiles() throws IOException {
        combineWithTempFiles(this.previousFilePath);
    }

    private void combineWithTempFiles(Path currentPVPath) throws IOException {
        if (!tempFiles.isEmpty()) {
            logger.debug("parquet combineWithTempFiles  currentPVPath sizes {} {} tempFiles {} ", Files.size(currentPVPath), currentPVPath, tempFiles);

            moveCurrentFile(currentPVPath);
            Configuration conf = new Configuration();
            RewriteOptions rewriteOptions = new RewriteOptions.Builder(
                    conf,
                    tempFiles.stream()
                            .map(p -> new org.apache.hadoop.fs.Path(p.toUri()))
                            .toList(),
                    new org.apache.hadoop.fs.Path(currentPVPath.toUri()))
                    .build();
            try {
                ParquetRewriter rewriter = new ParquetRewriter(rewriteOptions);
                rewriter.processBlocks();
                rewriter.close();
            } catch (IOException e) {
                logger.error("Failed to combine temporary files {}", tempFiles, e);
                throw e;
            }
            // Delete tempFiles
            tempFiles.forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    logger.error("Failed to delete {}", p, e);
                }
            });
            tempFiles.clear();
        }
    }
}
