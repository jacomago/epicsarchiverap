package org.epics.archiverappliance;

import org.epics.archiverappliance.retrieval.pva.PvaGetPVDataTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTests.class)

public class TomcatShutdownTest {
    static TomcatSetup tomcatSetup = new TomcatSetup();

    @BeforeClass
    public static void setup() throws Exception {

        tomcatSetup.setUpWebApps(PvaGetPVDataTest.class.getSimpleName());
    }

    @Test
    public void testTomcatShutdown() throws Exception {
        tomcatSetup.tearDown();
    }
}
