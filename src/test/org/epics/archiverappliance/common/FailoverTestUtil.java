package org.epics.archiverappliance.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.config.exception.AlreadyRegisteredException;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.beans.IntrospectionException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.epics.archiverappliance.TomcatSetup.BUILD_TOMCATS_TOMCAT;
import static org.epics.archiverappliance.TomcatSetup.FAILOVER_DEST_PORT;
import static org.epics.archiverappliance.TomcatSetup.FAILOVER_OTHER_PORT;

public class FailoverTestUtil {
    private static final Logger logger = LogManager.getLogger(FailoverTestUtil.class.getName());

    public static int generateData(
            String applianceName,
            Instant lastMonth,
            int startingOffset,
            ConfigService configService,
            String pvName,
            long stepSeconds,
            String testName,
            String etlKey,
            PartitionGranularity partitionGranularity)
            throws IOException {
        int genEventCount = 0;
        StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=" + etlKey + "&rootFolder=" + BUILD_TOMCATS_TOMCAT
                        + testName + "/" + applianceName + "/" + etlKey.toLowerCase()
                        + "&partitionGranularity=PARTITION_DAY",
                configService);
        Instant start = TimeUtils.getPreviousPartitionLastSecond(lastMonth, partitionGranularity)
                .plusSeconds(1 + startingOffset);
        Instant end = TimeUtils.getNextPartitionFirstSecond(lastMonth, partitionGranularity);
        try (BasicContext context = new BasicContext()) {
            ArrayListEventStream strm = new ArrayListEventStream(
                    0,
                    new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, TimeUtils.getYear(lastMonth)));
            for (Instant s = start; // We generate a months worth of data.
                    s.isBefore(end);
                    s = s.plusSeconds(stepSeconds)) {
                strm.add(new POJOEvent(
                        ArchDBRTypes.DBR_SCALAR_DOUBLE, s, new ScalarValue<Double>((double) s.getEpochSecond()), 0, 0));
            }
            genEventCount = plugin.appendData(context, pvName, strm);
        }
        logger.info(
                "Generated data for appliance " + applianceName + " pv " + pvName + " from " + start + " to " + end);
        return genEventCount;
    }

    public static int generateData(
            String applianceName,
            Instant lastMonth,
            int startingOffset,
            ConfigService configService,
            String pvName,
            long stepSeconds,
            String testName,
            String etlKey)
            throws IOException {
        return generateData(
                applianceName,
                lastMonth,
                startingOffset,
                configService,
                pvName,
                stepSeconds,
                testName,
                etlKey,
                PartitionGranularity.PARTITION_MONTH);
    }

    public static PVTypeInfo generatePVTypeInfo(
            String applianceName, ConfigService configService, String pvName, String otherURL, boolean paused)
            throws IOException, IllegalAccessException, InvocationTargetException, IntrospectionException,
                    NoSuchMethodException, InstantiationException {

        JSONObject srcPVTypeInfoJSON = (JSONObject) JSONValue.parse(
                new InputStreamReader(new FileInputStream("src/resources/test/data/PVTypeInfoPrototype.json")));
        PVTypeInfo destPVTypeInfo = new PVTypeInfo();
        JSONDecoder<PVTypeInfo> decoder = JSONDecoder.getDecoder(PVTypeInfo.class);
        decoder.decode(srcPVTypeInfoJSON, destPVTypeInfo);
        destPVTypeInfo.setPaused(paused);
        destPVTypeInfo.setPvName(pvName);
        destPVTypeInfo.setApplianceIdentity(applianceName);
        destPVTypeInfo.setChunkKey(configService.getPVNameToKeyConverter().convertPVNameToKey(pvName));
        destPVTypeInfo.setCreationTime(TimeUtils.convertFromISO8601String("2020-11-11T14:49:58.523Z"));
        destPVTypeInfo.setModificationTime(TimeUtils.now());
        if (otherURL != null && !otherURL.isEmpty()) {
            destPVTypeInfo.getDataStores()[1] = "merge://localhost?name=MTS&dest="
                    + URLEncoder.encode(destPVTypeInfo.getDataStores()[1], StandardCharsets.UTF_8)
                    + "&other=" + URLEncoder.encode(otherURL, StandardCharsets.UTF_8);
        }
        return destPVTypeInfo;
    }

    public static void updatePVTypeInfo(
            String applURL, String applianceName, ConfigService configService, String pvName, String newMTS)
            throws IOException, IllegalAccessException, InvocationTargetException, IntrospectionException,
                    NoSuchMethodException, InstantiationException {
        PVTypeInfo destPVTypeInfo = generatePVTypeInfo(applianceName, configService, pvName, newMTS, true);
        JSONEncoder<PVTypeInfo> encoder = JSONEncoder.getEncoder(PVTypeInfo.class);

        GetUrlContent.postObjectAndGetContentAsJSONObject(
                applURL + "/mgmt/bpl/putPVTypeInfo?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8)
                        + "&override=true&createnew=true",
                encoder.encode(destPVTypeInfo));
        logger.info("Added " + pvName + " to the appliance " + applianceName);
    }

    public static void updateLocalPVTypeInfo(
            String applianceName, ConfigService configService, String pvName, String newMTS)
            throws IOException, IllegalAccessException, InvocationTargetException, IntrospectionException,
                    NoSuchMethodException, InstantiationException, AlreadyRegisteredException {
        PVTypeInfo destPVTypeInfo = generatePVTypeInfo(applianceName, configService, pvName, newMTS, false);
        configService.updateTypeInfoForPV(pvName, destPVTypeInfo);
        configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
        configService.getETLLookup().manualControlForUnitTests();
        logger.info("Added " + pvName + " to the appliance " + applianceName);
    }

    public static void updatePVTypeInfo(
            String applURL, String applianceName, ConfigService configService, String pvName)
            throws IOException, IllegalAccessException, InvocationTargetException, IntrospectionException,
                    NoSuchMethodException, InstantiationException {
        updatePVTypeInfo(applURL, applianceName, configService, pvName, null);
    }

    public static void changeMTSForDest(String pvName) throws Exception {
        JSONObject srcPVTypeInfoJSON = GetUrlContent.getURLContentAsJSONObject("http://localhost:" + FAILOVER_DEST_PORT
                + "/mgmt/bpl/getPVTypeInfo?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8));
        JSONDecoder<PVTypeInfo> decoder = JSONDecoder.getDecoder(PVTypeInfo.class);
        JSONEncoder<PVTypeInfo> encoder = JSONEncoder.getEncoder(PVTypeInfo.class);
        PVTypeInfo destPVTypeInfo = new PVTypeInfo();
        decoder.decode(srcPVTypeInfoJSON, destPVTypeInfo);
        String otherURL = "pbraw://localhost?name=MTS&rawURL="
                + URLEncoder.encode(
                        "http://localhost:" + FAILOVER_OTHER_PORT + "/retrieval/data/getData.raw",
                        StandardCharsets.UTF_8);
        destPVTypeInfo.getDataStores()[1] = "merge://localhost?name=MTS&dest="
                + URLEncoder.encode(destPVTypeInfo.getDataStores()[1], StandardCharsets.UTF_8)
                + "&other=" + URLEncoder.encode(otherURL, StandardCharsets.UTF_8);
        logger.info("Data store is " + destPVTypeInfo.getDataStores()[1]);

        GetUrlContent.postObjectAndGetContentAsJSONObject(
                "http://localhost:" + FAILOVER_DEST_PORT + "/mgmt/bpl/putPVTypeInfo?pv="
                        + URLEncoder.encode(pvName, StandardCharsets.UTF_8) + "&override=true&createnew=true",
                encoder.encode(destPVTypeInfo));
        logger.info("Changed " + pvName + " to a merge dedup plugin");
    }
}
