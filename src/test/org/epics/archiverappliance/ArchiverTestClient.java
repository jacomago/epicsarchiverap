package org.epics.archiverappliance;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.epics.archiverappliance.common.PVStatus;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class ArchiverTestClient {
    public static final String ArchiveEndPoint = "archivePV";
    public static final String ACCESS_URL =
            "http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT + "/retrieval/data/getData.raw";
    private static final Logger logger = LogManager.getLogger(ArchiverTestClient.class);
    private static final String mgmtUrl = "http://localhost:17665/mgmt/bpl/";

    public static void waitForStatusChange(String pvName, PVStatus expectedStatus) {

        waitForStatusChange(List.of(pvName), expectedStatus, 10, 5);
    }

    public static void waitForStatusChange(String pvName, PVStatus expectedStatus, int maxTries) {
        waitForStatusChange(List.of(pvName), expectedStatus, maxTries, 5);
    }

    public static void waitForStatusChange(
            List<String> pvNames, PVStatus expectedStatus, int maxTries, long waitPeriodSeconds) {
        Awaitility.await()
                .pollInterval(waitPeriodSeconds, TimeUnit.SECONDS)
                .atMost(maxTries * waitPeriodSeconds, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    List<PVStatus> statuses = getCurrentStatus(pvNames);
                    Assertions.assertTrue(statuses.stream().allMatch(s -> Objects.equals(s, expectedStatus)));
                });
    }

    public static JSONArray getCurrentPVStatus(List<String> pvNames) throws IOException {
        // Check archiving
        String statusPVURL = mgmtUrl + "getPVStatus";
        return GetUrlContent.postStringListAndGetJSON(statusPVURL, "pv", pvNames);
    }

    public static List<PVStatus> getCurrentStatus(List<String> pvNames) throws IOException {
        JSONArray pvStatus = getCurrentPVStatus(pvNames);
        if (pvStatus == null) {
            logger.info("statuses are " + null);

            return Collections.nCopies(pvNames.size(), PVStatus.NOT_BEING_ARCHIVED);
        }
        List<PVStatus> currentStatuses = new ArrayList<>();
        for (var ob : pvStatus) {
            JSONObject obj = (JSONObject) ob;
            currentStatuses.add(PVStatus.fromString(obj.get("status").toString()));
        }
        logger.info("statuses are " + currentStatuses);
        return currentStatuses;
    }

    public static void requestAction(List<String> pvs, String endPoint, PVStatus expectedStatus) throws IOException {

        String fullUrl = mgmtUrl + endPoint;
        GetUrlContent.postStringListAndGetJSON(fullUrl, "pv", pvs);
        waitForStatusChange(pvs, expectedStatus, 20, 10);
    }

    public static void archivePVs(List<String> pvs, String samplingMethod, String samplingPeriod) throws IOException {
        logger.info("Submitting " + pvs + " to be archived.");
        String fullUrl = mgmtUrl + ArchiveEndPoint;
        JSONArray jsonArray = new JSONArray();
        pvs.forEach(pv -> jsonArray.add(
                new JSONObject(Map.of("pv", pv, "samplingperiod", samplingPeriod, "samplingmethod", samplingMethod))));
        GetUrlContent.postAndGetContent(fullUrl, jsonArray);
        waitForStatusChange(pvs, PVStatus.BEING_ARCHIVED, 20, 10);
    }

    public static void archivePVs(List<String> pvs) throws IOException {
        logger.info("Submitting " + pvs + " to be archived.");

        requestAction(pvs, ArchiveEndPoint, PVStatus.BEING_ARCHIVED);
    }

    public static void archivePV(String pv) throws IOException {
        archivePVs(List.of(pv));
    }

    public static void pausePVs(List<String> pvs) throws IOException {
        logger.info("Submitting " + pvs + " to be paused.");

        requestAction(pvs, "pauseArchivingPV", PVStatus.PAUSED);
    }

    public static void pausePV(String pv) throws IOException {
        pausePVs(List.of(pv));
    }

    public static void resumePVs(List<String> pvs) throws IOException {
        logger.info("Submitting " + pvs + " to be resumed.");

        requestAction(pvs, "resumeArchivingPV", PVStatus.BEING_ARCHIVED);
    }

    public static void resumePV(String pv) throws IOException {
        resumePVs(List.of(pv));
    }

    public static void deletePVs(List<String> pvs, boolean deleteData) throws IOException {
        logger.info("Submitting " + pvs + " to be deleted.");

        String fullUrl = mgmtUrl + "deletePV";
        if (deleteData) {
            fullUrl = fullUrl + "?deleteData=true";
        }
        JSONArray jsonArray = new JSONArray();
        jsonArray.addAll(pvs);
        GetUrlContent.postAndGetContent(fullUrl, jsonArray);
        waitForStatusChange(pvs, PVStatus.NOT_BEING_ARCHIVED, 20, 10);
    }

    public static void deletePVs(List<String> pvs) throws IOException {
        deletePVs(pvs, false);
    }

    public static void deletePV(String pv) throws IOException {
        deletePVs(List.of(pv));
    }

    public static void aliasPV(String pv, String aliasName) {
        logger.info("Submitting " + pv + " to be aliased by " + aliasName);

        String fullUrl = mgmtUrl + "addAlias?pv=" + pv + "&aliasname=" + aliasName;
        GetUrlContent.getURLContentAsJSONObject(fullUrl);
        waitForStatusChange(List.of(aliasName), PVStatus.BEING_ARCHIVED, 10, 10);
    }

    public static void removeAliasPV(String pv, String aliasName) {
        String fullUrl = mgmtUrl + "removeAlias?pv=" + URLEncoder.encode(pv, StandardCharsets.UTF_8) + "&aliasname="
                + URLEncoder.encode(aliasName, StandardCharsets.UTF_8);
        GetUrlContent.getURLContentAsJSONObject(fullUrl);
        waitForStatusChange(List.of(pv), PVStatus.BEING_ARCHIVED, 10, 10);
        waitForStatusChange(List.of(aliasName), PVStatus.NOT_BEING_ARCHIVED, 10, 10);
    }

    public static void changeParams(String pv, String samplingperiod, String samplingmethod) {
        String fullUrl = mgmtUrl + "changeArchivalParameters?pv=" + pv + "&samplingperiod=" + samplingperiod
                + "&samplingmethod=" + samplingmethod;
        GetUrlContent.getURLContentAsJSONObject(fullUrl);
    }

    public static void cleanUpPVs(List<String> pvs) throws IOException {
        JSONArray statuses = getCurrentPVStatus(pvs);
        List<String> cleanPVs = statuses.stream()
                .filter(s -> {
                    PVStatus status =
                            PVStatus.fromString(((JSONObject) s).get("status").toString());
                    return List.of(PVStatus.BEING_ARCHIVED, PVStatus.PAUSED).contains(status);
                })
                .map(s -> ((JSONObject) s).get("pvName").toString())
                .toList();
        pausePVs(cleanPVs);
        deletePVs(cleanPVs, true);
    }
    /**
     * Get data count
     */
    public static long retrievalCount(String pvName, Instant start, Instant end) throws IOException {
        RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream(ACCESS_URL);

        try (EventStream stream = rawDataRetrieval.getDataForPVS(new String[] {pvName}, start, end, null)) {
            long previousEpochSeconds = 0;
            int eventCount = 0;

            // We are making sure that the stream we get back has times in sequential order...
            if (stream != null) {
                for (Event e : stream) {
                    long actualSeconds = e.getEpochSeconds();
                    Assertions.assertTrue(actualSeconds >= previousEpochSeconds);
                    previousEpochSeconds = actualSeconds;
                    eventCount++;
                }
            }
            return eventCount;
        }
    }
}
