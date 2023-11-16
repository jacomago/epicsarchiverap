package org.epics.archiverappliance;

import org.epics.archiverappliance.retrieval.pva.PvaGetPVDataTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.Future;

@Category(IntegrationTests.class)

public class TomcatSetupTest {
    static TomcatSetup tomcatSetup = new TomcatSetup();

    @Test
    public void testTomcatShutdown() throws Exception {
        Future<?> future = tomcatSetup.setUpWebApps(PvaGetPVDataTest.class.getSimpleName(), true);
        tomcatSetup.tearDown();
        future.get();
    }
}
