/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.plain.utils;

import edu.stanford.slac.archiverappliance.plain.FileExtension;
import edu.stanford.slac.archiverappliance.plain.FileInfo;
import edu.stanford.slac.archiverappliance.plain.FileStreamCreator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Various validations for a PB file.
 * 1) Checks that the times in the PB file are sequential.
 *
 * @author mshankar
 *
 */
public class ValidatePBFile {
    private static final Logger logger = LogManager.getLogger(ValidatePBFile.class.getName());

    public static boolean validatePBFile(Path path, boolean verboseMode, FileExtension fileExtension)
            throws IOException {
        FileInfo info = FileInfo.extensionPath(fileExtension, path);
        logger.info("File " + path.getFileName().toString() + " is for PV " + info.getPVName() + " of type "
                + info.getType() + " for year " + info.getDataYear());
        Instant previousTimestamp = Instant.EPOCH;
        long eventnum = 0;
        try (EventStream strm = FileStreamCreator.getStream(fileExtension, info.getPVName(), path, info.getType())) {
            Event firstEvent = null;
            Event lastEvent = null;
            for (Event ev : strm) {
                Instant eventTimeStamp = ev.getEventTimeStamp();
                if (eventTimeStamp.isAfter(previousTimestamp) || eventTimeStamp.equals(previousTimestamp)) {
                    previousTimestamp = eventTimeStamp;
                } else {
                    throw new IOException("We expect to see monotonically increasing timestamps in a PB file"
                            + ". This is not true at " + eventnum
                            + ". The previous time stamp is "
                            + previousTimestamp
                            + ". The current time stamp is "
                            + eventTimeStamp);
                }
                if (firstEvent == null) firstEvent = ev;
                lastEvent = ev;
                eventnum++;
            }

            if (verboseMode) {
                assert firstEvent != null;
                logger.info("File " + path.getFileName().toString() + " appears to be valid. It has " + eventnum
                        + " events ranging from "
                        + firstEvent.getEventTimeStamp()
                        + " to "
                        + lastEvent.getEventTimeStamp());
            }
            if (verboseMode) {
                System.out.println(path + " seems to be a valid PB file.");
            }
            return true;
        } catch (Exception ex) {
            if (verboseMode) {
                logger.error("Exception parsing file " + path.toAbsolutePath() + "; eventnum=" + eventnum, ex);
            }
            System.out.println(path + " is an invalid PB file.");
            return false;
        }
    }

    /**
     * @param args  &emsp;
     * @throws Exception  &emsp;
     */
    public static void main(String[] args) throws Exception {

        if (args == null || args.length <= 0) {
            printHelpMsg();
            return;
        }

        boolean verboseMode = false;
        LinkedList<String> argsAfterOptions = new LinkedList<String>();
        for (String arg : args) {
            if (arg.equals("-v")) {
                verboseMode = true;
            } else if (arg.equals("-h")) {
                printHelpMsg();
                return;
            } else {
                argsAfterOptions.add(arg);
            }
        }

        // Use AtomicInteger to allow use inside anonymous class
        final AtomicInteger failures = new AtomicInteger(0);

        for (String fileName : argsAfterOptions) {
            Path path = Paths.get(fileName);
            if (Files.isDirectory(path)) {
                Files.walkFileTree(
                        path,
                        new FileVisitor<Path>() {
                            private boolean verboseMode = false;

                            FileVisitor<Path> init(boolean verboseMode) {
                                this.verboseMode = verboseMode;
                                return this;
                            }

                            @Override
                            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                                    throws IOException {
                                return FileVisitResult.CONTINUE;
                            }

                            @Override
                            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                                if (!validatePBFile(file, verboseMode, FileExtension.PB)) {
                                    failures.incrementAndGet();
                                }
                                return FileVisitResult.CONTINUE;
                            }

                            @Override
                            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                                return FileVisitResult.CONTINUE;
                            }

                            @Override
                            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                                return FileVisitResult.CONTINUE;
                            }
                        }.init(verboseMode));
            } else {
                if (!validatePBFile(path, verboseMode, FileExtension.PB)) {
                    failures.incrementAndGet();
                }
            }
        }
        // Return number of failures as exit code.
        System.exit(failures.get());
    }

    private static void printHelpMsg() {
        System.out.println();
        System.out.println("Usage: validate.sh <options> <Any number of file or folder names>");
        System.out.println();
        System.out.println("\t-h Prints this help");
        System.out.println("\t-v Turns on verbose logging.");
        System.out.println();
        System.out.println();
        System.out.println();
    }
}
