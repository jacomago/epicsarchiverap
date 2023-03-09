package org.epics.archiverappliance.engine.V4;

import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.epics.archiverappliance.ArchiverTestClient.archivePV;
import static org.epics.archiverappliance.ArchiverTestClient.deletePVs;
import static org.epics.archiverappliance.ArchiverTestClient.pausePV;
import static org.epics.archiverappliance.ArchiverTestClient.resumePV;

/**
 * Checks pausing and resuming a pv keeps it using the pvaccess protocol.
 */
@Tag("integration")
@Tag("localEpics")
class PauseResumeV4Test {

    private SIOCSetup ioc;
    TomcatSetup tomcatSetup = new TomcatSetup();
    private static final String pvPrefix = PauseResumeV4Test.class.getSimpleName();

    @BeforeEach
    public void setUp() throws Exception {
        ioc = new SIOCSetup();
        ioc.startSIOCWithDefaultDB();

        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
    }

    String pvName = pvPrefix + "UnitTestNoNamingConvention:sine:calc";

    @AfterEach
    public void tearDown() throws Exception {
        pausePV(pvName);
        deletePVs(List.of(pvName), true);
        ioc.stopSIOC();
        tomcatSetup.tearDown();
    }

    @Test
    void testPauseRestart() throws Exception {
        String pvURLName = URLEncoder.encode(pvName, StandardCharsets.UTF_8);

        // Archive PV
        String mgmtUrl = "http://localhost:17665/mgmt/bpl/";
        archivePV("pva://" + pvName);

        usingPvAccessCheck(pvURLName, mgmtUrl);

        // Let's pause the PV.
        pausePV(pvName);

        // Resume PV
        resumePV(pvName);

        usingPvAccessCheck(pvURLName, mgmtUrl);
    }

    private void usingPvAccessCheck(String pvURLName, String mgmtUrl) {
        // Check using PVAccess
        String pvDetailsURL = mgmtUrl + "getPVDetails?pv=";
        JSONArray pvInfo = GetUrlContent.getURLContentAsJSONArray(pvDetailsURL + pvURLName);
        JSONObject pvAccessInfo = (JSONObject) pvInfo.get(12);
        Assertions.assertEquals("Are we using PVAccess?", pvAccessInfo.get("name"));
        Assertions.assertEquals("Yes", pvAccessInfo.get("value"));
    }
}
