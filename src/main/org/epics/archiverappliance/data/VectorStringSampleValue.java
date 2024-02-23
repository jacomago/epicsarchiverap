/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.data;

import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import org.json.simple.JSONValue;

/**
 * An implementation of SampleValue for vector strings.
 * @author mshankar
 *
 */
public class VectorStringSampleValue implements SampleValue {
	private final List<String> values;

	public VectorStringSampleValue(List<String> values) {
		this.values = values;
	}
	
	/* (non-Javadoc)
	 * The toString for vectors generates a a JSON vector...
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		boolean first = true;
		StringWriter buf = new StringWriter();
		buf.append('[');
		for(String value : values) {
			if(first) { first = false; } else { buf.append(","); }
			buf.append(value);
		}
		buf.append(']');
		return buf.toString();		
	}

	@Override
	public Number getValue() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getElementCount() {
		return values.size();
	}

	@Override
	public Number getValue(int index) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public String getStringValue(int index) {
		return values.get(index);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public List getValues() {
		return values;
	}

	@Override
	public List<String> getStringValues() {
		return values;
	}

	@Override
	public <T extends Number> List<T> getNumberValues() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int hashCode() {
		return values.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof VectorStringSampleValue that)) return false;
        return Objects.equals(getValues(), that.getValues());
	}

	@Override
	public String toJSONString() {
		boolean first = true;
		StringWriter buf = new StringWriter();
		buf.append('[');
		for(String value : values) {
			if(!value.isEmpty()) { 
				if(first) { first = false; } else { buf.append(","); }
				buf.append("\"");
				buf.append(JSONValue.escape(value));
				buf.append("\"");
			}
		}
		buf.append(']');
		return buf.toString();
	}

	@Override
	public ByteBuffer getValueAsBytes() {
		throw new UnsupportedOperationException();
	}
}
