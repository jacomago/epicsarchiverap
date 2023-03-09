package org.epics.archiverappliance.config;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class PVNameToKeyConverterTest {

	@Test
	public void testKeyName() throws Exception {
		DefaultConfigService configService = new ConfigServiceForTests(-1);
        String expectedKeyName = "A/B/C/D:";
        String keyName = configService.getPVNameToKeyConverter().convertPVNameToKey("A:B:C-D"); 
		assertTrue("We were expecting " + expectedKeyName + " instead we got " + keyName, expectedKeyName.equals(keyName));
	}

}
