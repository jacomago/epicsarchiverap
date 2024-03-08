package edu.stanford.slac.archiverappliance.plain.pb;

import edu.stanford.slac.archiverappliance.plain.AppendDataStateData;
import edu.stanford.slac.archiverappliance.plain.FileInfo;
import edu.stanford.slac.archiverappliance.plain.PathResolver;
import edu.stanford.slac.archiverappliance.plain.PlainFileHandler;
import edu.stanford.slac.archiverappliance.plain.URLKeys;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.PVNameToKeyMapping;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.etl.common.DefaultETLInfoListProcessor;
import org.epics.archiverappliance.etl.common.ETLInfoListProcessor;
import org.epics.archiverappliance.utils.nio.ArchPaths;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;

public class PBPlainFileHandler implements PlainFileHandler {
    public static final String PB_PLUGIN_IDENTIFIER = "pb";
    public static final String pbFileExtension = ".pb";

    private PBCompressionMode compressionMode = PBCompressionMode.NONE;

    @Override
    public String pluginIdentifier() {
        return PB_PLUGIN_IDENTIFIER;
    }

    @Override
    public PathResolver getPathResolver() {
        return switch (compressionMode) {
            case NONE -> PathResolver.BASE_PATH_RESOLVER;
            case ZIP_PER_PV -> (paths, createParentFolder, rootFolder, pvComponent, pvKey) ->
                    paths.get(createParentFolder, rootFolder, pvKey + "_pb.zip!", pvComponent);
        };
    }

    @Override
    public boolean useSearchForPositions() {
        return this.compressionMode.equals(PBCompressionMode.NONE);
    }

    @Override
    public String rootFolderPath(String rootFolder) {
        return this.compressionMode.equals(PBCompressionMode.NONE)
                ? rootFolder
                : rootFolder.replace(ArchPaths.ZIP_PREFIX, "/");
    }

    @Override
    public void initCompression(Map<String, String> queryStrings) {
        this.compressionMode = PBCompressionMode.valueOf(queryStrings.get(URLKeys.COMPRESS.key()));
    }

    @Override
    public FileInfo fileInfo(Path path) throws IOException {
        return new PBFileInfo(path);
    }

    @Override
    public String toString() {
        return "PBPlainFileHandler{" + "compressionMode=" + compressionMode + '}';
    }

    @Override
    public EventStream getTimeStream(
            String pvName, Path path, ArchDBRTypes dbrType, Instant start, Instant end, boolean skipSearch)
            throws IOException {
        return new FileBackedPBEventStream(pvName, path, dbrType, start, end, skipSearch);
    }

    @Override
    public EventStream getTimeStream(
            String pvName, Path path, Instant start, Instant end, boolean skipSearch, FileInfo fileInfo)
            throws IOException {
        return new FileBackedPBEventStream(pvName, path, fileInfo.getType(), start, end, skipSearch);
    }

    @Override
    public EventStream getStream(String pvName, Path path, ArchDBRTypes dbrType) throws IOException {
        return new FileBackedPBEventStream(pvName, path, dbrType);
    }

    @Override
    public AppendDataStateData appendDataStateData(
            Instant timestamp,
            PartitionGranularity partitionGranularity,
            String rootFolder,
            String desc,
            PVNameToKeyMapping pv2key) {
        return new PBAppendDataStateData(
                partitionGranularity, rootFolder, desc, timestamp, compressionMode, pv2key, getPathResolver());
    }

    @Override
    public void markForDeletion(Path path) {
        // Nothing for PB files
    }

    @Override
    public void dataMovePaths(
            BasicContext context,
            String pvName,
            String randSuffix,
            String suffix,
            String rootFolder,
            PVNameToKeyMapping pv2key)
            throws IOException {
        PlainFileHandler.movePaths(context, pvName, randSuffix, suffix, rootFolder, getPathResolver(), pv2key);
    }

    @Override
    public void dataDeleteTempFiles(
            BasicContext context, String pvName, String randSuffix, String rootFolder, PVNameToKeyMapping pv2key)
            throws IOException {
        PlainFileHandler.deleteTempFiles(context, pvName, randSuffix, rootFolder, getPathResolver(), pv2key);
    }

    @Override
    public ETLInfoListProcessor optimisedETLInfoListProcessor(ETLDest etlDest) {
        return new DefaultETLInfoListProcessor(etlDest);
    }

    @Override
    public String updateRootFolderStr(String rootFolderStr) {
        if (compressionMode.equals(PBCompressionMode.ZIP_PER_PV)) {
            if (!rootFolderStr.startsWith(ArchPaths.ZIP_PREFIX)) {
                String rootFolderWithPath = ArchPaths.ZIP_PREFIX + rootFolderStr;
                logger.debug("Automatically adding url scheme for compression to rootfolder " + rootFolderWithPath);
                return rootFolderWithPath;
            }
        }
        return rootFolderStr;
    }

    @Override
    public boolean backUpFiles(boolean backupFilesBeforeETL) {
        return this.compressionMode.equals(PBCompressionMode.NONE) && backupFilesBeforeETL;
    }

    @Override
    public Map<URLKeys, String> urlOptions() {
        if (compressionMode.equals(PBCompressionMode.NONE)) {
            return Map.of();
        }
        return Map.of(URLKeys.COMPRESS, compressionMode.name());
    }

    @Override
    public String getPathKey(Path path) {
        if (this.compressionMode.equals(PBCompressionMode.NONE)) {
            return path.toAbsolutePath().toString();
        }
        return URLDecoder.decode(path.toUri().toString(), StandardCharsets.US_ASCII)
                .replace(" ", "+");
    }
}
