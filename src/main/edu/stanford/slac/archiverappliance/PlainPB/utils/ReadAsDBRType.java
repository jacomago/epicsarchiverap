package edu.stanford.slac.archiverappliance.PlainPB.utils;

import edu.stanford.slac.archiverappliance.PB.utils.LineByteStream;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;

/**
 * Small utilty to read a .pb file as the specfied DBR type.
 * This skips the PBFileInfo; so it should work better against corrupted files.
 * @author mshankar
 *
 */
public class ReadAsDBRType {

	public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		if(args.length < 3) {
			System.err.println("Usage: java edu.stanford.slac.archiverappliance.PlainPB.utils.ReadAsDBRType <DBRType> <Year> <File>");
			System.err.println("DBRType is something that can be passed into ArchDBRTypes.valueOf. For example, DBR_SCALAR_DOUBLE ");
			return;
		}
		
		ArchDBRTypes archDBRType = ArchDBRTypes.valueOf(args[0]);
		short year = Short.parseShort(args[1]);
		String path = args[2];

		ByteArray bar = new ByteArray(LineByteStream.MAX_LINE_SIZE);
		try(LineByteStream lis = new LineByteStream(Paths.get(path))) {
			lis.readLine(bar);
			while(lis.readLine(bar) != null && !bar.isEmpty()) { 
				Event event = Event.fromByteArray(archDBRType, bar, year);
				System.out.println(TimeUtils.convertToHumanReadableString(event.instant()) + " ==> " + event.value().toString());
			}
		}
	}

}
