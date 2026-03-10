package org.epics.archiverappliance.taglets;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.SimpleElementVisitor9;

import com.sun.source.doctree.DocTree;
import com.sun.source.doctree.DocTree.Kind;
import com.sun.source.doctree.TextTree;
import com.sun.source.doctree.UnknownBlockTagTree;
import com.sun.source.util.SimpleDocTreeVisitor;

import jdk.javadoc.doclet.Doclet;
import jdk.javadoc.doclet.DocletEnvironment;
import jdk.javadoc.doclet.Taglet;

/**
 * Taglet for the action itself. 
 * @author mshankar
 *
 */
public class BPLActionTaglet implements Taglet {
    private static final String NAME = "epics.BPLAction";
    public static Properties pathMappings = new Properties();
    public static Properties reversePathMappings = new Properties();
    
    public BPLActionTaglet() {
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
		buf.append("<div class=\"subNav\">Access using: <code>http://<i>mgmt_url</i>/bpl");
		String clazz = element.accept(new SimpleElementVisitor9<String, Integer>(){
			@Override
			public String visitType(TypeElement e, Integer p) {
				return e.getQualifiedName().toString();
			}
		}, 0);
		String path = reversePathMappings.getProperty(clazz);
		buf.append(path);
		buf.append("</code> ");
		StringBuilder tagDesc = new StringBuilder();
		for(DocTree tag : tags) {
			if(tag.getKind() != Kind.UNKNOWN_BLOCK_TAG) {
				System.out.println("Unknown tag kind in " + getClass().getName() + ":" + tag.getKind());
			}
			tagDesc.append(tag.accept(new SimpleDocTreeVisitor<String, Integer>(){
            	public String visitUnknownBlockTag(UnknownBlockTagTree btt, Integer p) {
            		StringBuilder unStr = new StringBuilder();
            		for(DocTree childTag : btt.getContent()) {
            			if(childTag.getKind() != Kind.TEXT) {
            				System.out.println("Unknown child tag kind in " + getClass().getName() + ":" + childTag.getKind());
            			}
            			unStr.append(childTag.accept(new SimpleDocTreeVisitor<String, Integer>(){
            				public String visitText(TextTree node, Integer p) {
            					return node.getBody();
            				}
            			}, 0));
            		}
            		return unStr.toString();
            	}
			}, 0));
		}
		String tagDescStr = tagDesc.toString();
		buf.append(tagDescStr);
		buf.append("<ul class=\"blockList\">");
		buf.append("</div>");

		BPLActionDetails.addMethod(path, clazz, tagDescStr);
		return buf.toString();
	}


	@Override
	public void init(DocletEnvironment env, Doclet doclet) {
		Taglet.super.init(env, doclet);
		
		try(FileInputStream fis = new FileInputStream(new File("docs/api/mgmtpathmappings.txt"))) { 
			pathMappings.load(fis);
			for(Object key : pathMappings.keySet()) { 
				String className = pathMappings.getProperty((String) key);
				reversePathMappings.put(className, key);
			}

			File bplactionitems = new File("docs/api/mgmt_scriptables.txt");
			if(bplactionitems.exists()) { 
				bplactionitems.delete();
			}
		} catch(Exception ex) { 
			throw new RuntimeException(ex);
		}	
	}    
}

