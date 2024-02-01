/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.search;

import edu.stanford.slac.archiverappliance.PB.utils.LineByteStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

/**
 * Test a file event stream
 * @author mshankar
 *
 */
public class FileEventStreamSearchTest {
    private static final Logger logger = LogManager.getLogger(FileEventStreamSearchTest.class);
    String pathName = ConfigServiceForTests.getDefaultPBTestFolder() + "/" + "FileEventStreamSearchTest.txt";
    Path path = Paths.get(pathName);

    // The exhaustive test takes about 30 minutes to run.
    // So, if we suspect something is wrong with searches, uncomment and run.
    // Otherwise, the testSeekToTime test should be more than adequate.
    //
    //	@Test
    //	public void exhaustiveTest() throws Exception {
    //		for(int i = 0; i < TestSampleGenerator.MAXSAMPLEINT; i++) {
    //			seekAndCheck(f, i);
    //		}
    //	}
    private static void seekAndCheck(Path path, final int searchNum) throws IOException {
        try {
            CompareEventLine compare = (line1, line2) -> {
                String inputline1 = new String(line1, StandardCharsets.UTF_8);
                int inputNum1 = Integer.parseInt(inputline1);
                int inputNum2 = Integer.MAX_VALUE;
                if (line2 != null) {
                    String inputline2 = new String(line2, StandardCharsets.UTF_8);
                    inputNum2 = Integer.parseInt(inputline2);
                }
                if (inputNum1 > searchNum) {
                    logger.debug("When searching for " + searchNum + ", comparing with " + inputNum1 + " and "
                            + inputNum2 + " sayz GO_LEFT 1");
                    return CompareEventLine.NextStep.GO_LEFT;
                } else if (inputNum2 < searchNum) {
                    logger.debug("When searching for " + searchNum + ", comparing with " + inputNum1 + " and "
                            + inputNum2 + " sayz GO_RIGHT 2");
                    return CompareEventLine.NextStep.GO_RIGHT;
                } else {
                    if (line2 != null) {
                        if (inputNum1 < searchNum) {
                            logger.debug("When searching for " + searchNum + ", comparing with " + inputNum1 + " and "
                                    + inputNum2 + " sayz STAY_WHERE_YOU_ARE 3");
                            return CompareEventLine.NextStep.STAY_WHERE_YOU_ARE;
                        } else {
                            logger.debug("When searching for " + searchNum + ", comparing with " + inputNum1 + " and "
                                    + inputNum2 + " sayz GO_LEFT 4");
                            return CompareEventLine.NextStep.GO_LEFT;
                        }
                    } else {
                        logger.debug("When searching for " + searchNum + ", comparing with " + inputNum1 + " and "
                                + inputNum2 + " sayz STAY_WHERE_YOU_ARE 5");
                        return CompareEventLine.NextStep.STAY_WHERE_YOU_ARE;
                    }
                }
            };

            FileEventStreamSearch bs = new FileEventStreamSearch(path, 0L);
            boolean found = bs.seekToTime(compare);
            if (!found) {
                if (searchNum >= 0 && searchNum < EvenNumberSampleFileGenerator.MAXSAMPLEINT) {
                    if (searchNum == 0) {
                        logger.debug(
                                "0 is a special case as it is the first item on the list and technically we did not find an event that satisfies the conditions.");
                    } else {
                        Assertions.fail("Failure when searching for " + searchNum);
                    }
                } // The number was out of range anyways...

            } else {
                // Check to see if s1 <= t1 < s2
                try (LineByteStream lis = new LineByteStream(path, bs.getFoundPosition())) {
                    lis.seekToFirstNewLine();
                    byte[] line1 = lis.readLine();
                    byte[] line2 = lis.readLine();
                    if (line1 == null || line2 == null || line1.length == 0 || line2.length == 0) {
                        if (searchNum >= 0 && searchNum < EvenNumberSampleFileGenerator.MAXSAMPLEINT) {
                            Assertions.fail("One of the lines was null but we could not find the number " + searchNum);
                        } // In this case, we really did not find the event as it is out of range.

                    } else {
                        int num1 = Integer.parseInt(new String(line1, StandardCharsets.UTF_8));
                        int num2 = Integer.parseInt(new String(line2, StandardCharsets.UTF_8));
                        // s1 <= t1 < s2
                        if (num1 < searchNum && searchNum <= num2) {

                        } else {
                            if (searchNum >= 0 && searchNum < EvenNumberSampleFileGenerator.MAXSAMPLEINT) {
                                Assertions.fail("Potential failure - could not locate " + searchNum);
                            } // In this case, we really did not find the event as it is out of range.
                        }
                    }
                }
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            System.err.println("Exception when searching for " + searchNum);
        }
    }

    @BeforeEach
    public void setUp() throws Exception {
        Files.deleteIfExists(path);

        EvenNumberSampleFileGenerator.generateSampleFile(pathName);
    }

    @Test
    public void testSeekToTime() throws Exception {
        // Check for lower boundary conditions
        for (int i = -1000; i < 1000; i++) {
            seekAndCheck(path, i);
        }
        // Test in the middle
        for (int i = EvenNumberSampleFileGenerator.MAXSAMPLEINT / 2;
                i < EvenNumberSampleFileGenerator.MAXSAMPLEINT / 2 + 1000;
                i++) {
            seekAndCheck(path, i);
        }

        // Check for upper boundary conditions
        for (int i = EvenNumberSampleFileGenerator.MAXSAMPLEINT - 1000;
                i < EvenNumberSampleFileGenerator.MAXSAMPLEINT + 1000;
                i++) {
            seekAndCheck(path, i);
        }

        // Test randomly
        Random random = new Random();
        for (int i = 0; i < 10000; i++) {
            seekAndCheck(path, random.nextInt(EvenNumberSampleFileGenerator.MAXSAMPLEINT));
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        Files.deleteIfExists(path);
    }
}
