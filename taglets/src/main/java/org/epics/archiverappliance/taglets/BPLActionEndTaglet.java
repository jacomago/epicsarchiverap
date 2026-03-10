package org.epics.archiverappliance.taglets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.lang.model.element.Element;

import com.sun.source.doctree.DocTree;

import jdk.javadoc.doclet.Taglet;


/**
 * Taglet for a param. 
 * @author mshankar
 *
 */
public class BPLActionEndTaglet implements Taglet {

    private static final String NAME = "epics.BPLActionEnd";
    
    public BPLActionEndTaglet() {
    }

	@Override
    public String getName() {
        return NAME;
    }

	@Override
    public boolean isInlineTag() {
        return false;
    }


	@Override
	public Set<Location> getAllowedLocations() {
		Set<Location> locations = new HashSet<Location>();
		locations.add(Location.TYPE);
		return locations;
	}

	@Override
	public String toString(List<? extends DocTree> tags, Element element) {
        if (tags.size() == 0) {
            return null;
        }

        StringBuilder buf = new StringBuilder();
        buf.append("</ul>");
        buf.append("</div>");
        
        BPLActionDetails.addMethodTerminator();
        
        return buf.toString();
	}    
}

