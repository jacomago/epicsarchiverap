package org.epics.archiverappliance.data;

public class ConversionException extends RuntimeException { 
	private static final long serialVersionUID = 2411190459945098387L;

	public ConversionException(String msg, Throwable cause) { 
		super(msg, cause);
	}
}