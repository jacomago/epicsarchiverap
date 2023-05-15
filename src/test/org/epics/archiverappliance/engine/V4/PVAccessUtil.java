package org.epics.archiverappliance.engine.V4;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.test.MemBufWriter;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig;
import org.epics.archiverappliance.mgmt.pva.actions.NTUtil;
import org.epics.archiverappliance.mgmt.pva.actions.NTUtilTest;
import org.epics.archiverappliance.mgmt.pva.actions.PvaGetPVStatus;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.pva.client.PVAChannel;
import org.epics.pva.data.*;
import org.epics.pva.data.nt.MustBeArrayException;
import org.epics.pva.data.nt.PVATable;
import org.epics.pva.data.nt.PVATimeStamp;
import org.epics.pva.server.ServerPV;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Assert;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class PVAccessUtil {

    public static Map<Instant, SampleValue> getReceivedValues(MemBufWriter writer, ConfigService configService) throws Exception {

        return getReceivedEvents(writer, configService).entrySet().stream()
                .map((e) -> Map.entry(e.getKey(), e.getValue().getSampleValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
    public static HashMap<Instant, Event> getReceivedEvents(MemBufWriter writer, ConfigService configService) throws Exception {
        double secondsToBuffer = configService.getEngineContext().getWritePeriod();
        // Need to wait for the writer to write all the received data.
        Thread.sleep((long) secondsToBuffer * 1000);

        HashMap<Instant, Event> actualValues = new HashMap<>();
        try {
            for (Event event : writer.getCollectedSamples()) {
                actualValues.put(event.getEventTimeStamp().toInstant(), event);
            }
        } catch (IOException e) {
            fail(e.getMessage());
        }
        return actualValues;
    }


    public static Map.Entry<Instant, PVAStructure> updateStructure(PVAStructure pvaStructure, ServerPV serverPV) {
        try {
            ((PVAStructure) pvaStructure.get("structure")).get("level 1").setValue(new PVAString("level 1", "level 1 0 new"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
        Instant instant = Instant.now();
        ((PVATimeStamp) pvaStructure.get("timeStamp")).set(instant);
        try {
            serverPV.update(pvaStructure);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        return Map.entry(instant, pvaStructure);
    }



    public static ArchiveChannel startArchivingPV(String pvName, MemBufWriter writer,
                                        ConfigService configService, ArchDBRTypes type) throws InterruptedException {
        return startArchivingPV(pvName, writer, configService, type, true);
    }

    public static ArchiveChannel startArchivingPV(String pvName, MemBufWriter writer,
                                        ConfigService configService, ArchDBRTypes type, boolean wait) throws InterruptedException {

        PVTypeInfo typeInfo = new PVTypeInfo(pvName, type, !type.isWaveForm(), 1);
        long samplingPeriodMilliSeconds = 100;
        float samplingPeriod = (float) samplingPeriodMilliSeconds / (float) 1000.0;
        try {
            ArchiveEngine.archivePV(pvName, samplingPeriod, PolicyConfig.SamplingMethod.MONITOR, 10, writer,
                    configService,
                    type, null, typeInfo.getArchiveFields(), true, false);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        ArchiveChannel pvChannel = configService.getEngineContext().getChannelList().get(pvName);
        try {
            pvChannel.startUpMetaChannels();
        } catch (Exception e) {
            fail(e.getMessage());
        }

        if (wait) {

            waitForIsConnected(pvChannel);

            // Update no fields

            Thread.sleep(samplingPeriodMilliSeconds + 1000);
        }
        return pvChannel;
    }

    public static void waitForIsConnected(ArchiveChannel pvChannel) {
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertTrue(!pvChannel.metaChannelsNeedStartingUp()
                        && pvChannel.isConnected()));
    }

    /**
     * to string formats as "value type_name actual_value"
     */
    public static String formatInput(PVAData value) {
        String[] splitStrings = value.toString().split(" ");
        String[] subArray = ArrayUtils.subarray(splitStrings, 2, splitStrings.length);

        return String.join("", subArray);
    }

    public static void waitForStatusChange(String pvName, String expectedStatus, int maxTries, String mgmtUrl, Logger logger) {
        waitForStatusChange(pvName, expectedStatus, maxTries, mgmtUrl, logger, 5);
    }

    public static void waitForStatusChange(String pvName, String expectedStatus, int maxTries, String mgmtUrl, Logger logger, long waitPeriodSeconds) {
        Awaitility.await()
                .pollInterval(waitPeriodSeconds, TimeUnit.SECONDS)
                .atMost(maxTries * waitPeriodSeconds, TimeUnit.SECONDS)
                .untilAsserted(() ->
                        Assert.assertEquals(
                                expectedStatus,
                                getCurentStatus(pvName, mgmtUrl, logger)
                        )
                );
    }

    private static String getCurentStatus(String pvName, String mgmtUrl, Logger logger) {
        String curentStatus;
        // Check archiving
        String statusPVURL = mgmtUrl + "getPVStatus?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8);
        JSONArray pvStatus = GetUrlContent.getURLContentAsJSONArray(statusPVURL);
        curentStatus = ((JSONObject) pvStatus.get(0)).get("status").toString();
        logger.debug("status is " + curentStatus);
        return curentStatus;
    }


    /**
     * Bytes to string method for debugging the byte buffers.
     *
     * @param data Input bytes
     * @return String representation as hexbytes and ascii conversion
     */
    public static String bytesToString(final ByteBuffer data) {
        ByteBuffer buffer = data.duplicate();

        return Hexdump.toHexdump(buffer);
    }

    private static PVAData extracted(SampleValue sampleValue, PVATypeRegistry types) throws Exception {
        ByteBuffer bytes = sampleValue.getValueAsBytes();
        var val = types.decodeType("struct name", bytes);
        val.decode(types, bytes);
        return val;
    }
    public static Map<Instant, PVAData> convertBytesToPVAStructure(Map<Instant, SampleValue> actualValues) {
        PVATypeRegistry types = new PVATypeRegistry();
        return actualValues.entrySet().stream().map((e) -> {
            try {
                return Map.entry(e.getKey(), extracted(e.getValue(), types));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static PVAStructure getCurrentStatus(List<String> pvNames, PVAChannel pvaChannel) throws ExecutionException,
            InterruptedException, TimeoutException, MustBeArrayException {

        PVATable archivePvStatusReqTable = PVATable.PVATableBuilder.aPVATable().name(PvaGetPVStatus.NAME)
                .descriptor(PvaGetPVStatus.NAME)
                .addColumn(new PVAStringArray("pv", pvNames.toArray(new String[pvNames.size()])))
                .build();
        return pvaChannel.invoke(archivePvStatusReqTable).get(30, TimeUnit.SECONDS);
    }

    public static HashMap<String, String> getStatuses(List<String> pvNamesAll, PVAChannel pvaChannel) throws ExecutionException, InterruptedException, TimeoutException, MustBeArrayException {
        var statuses = NTUtil.extractStringArray(PVATable
                .fromStructure(getCurrentStatus(pvNamesAll, pvaChannel))
                .getColumn("status"));
        var pvs = NTUtil.extractStringArray(PVATable
                .fromStructure(getCurrentStatus(pvNamesAll, pvaChannel))
                .getColumn("pv"));
        var result = new HashMap<String, String>();
        for (int i = 0; i<pvs.length; i++) {
            result.put(pvs[i], statuses[i]);
        }
        return result;
    }
}