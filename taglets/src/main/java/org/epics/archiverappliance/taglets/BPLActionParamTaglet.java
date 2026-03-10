package org.epics.archiverappliance.taglets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.lang.model.element.Element;

import com.sun.source.doctree.AttributeTree;
import com.sun.source.doctree.DocTree;
import com.sun.source.doctree.DocTree.Kind;
import com.sun.source.doctree.EndElementTree;
import com.sun.source.doctree.EntityTree;
import com.sun.source.doctree.StartElementTree;
import com.sun.source.doctree.TextTree;
import com.sun.source.doctree.UnknownBlockTagTree;
import com.sun.source.util.SimpleDocTreeVisitor;

import jdk.javadoc.doclet.Taglet;


/**
 * Taglet for a param. 
 * @author mshankar
 *
 */
public class BPLActionParamTaglet implements Taglet {

    private static final String NAME = "epics.BPLActionParam";
    
    public BPLActionParamTaglet() {
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
        for(DocTree tag : tags) {
            buf.append("<li>");
			if(tag.getKind() != Kind.UNKNOWN_BLOCK_TAG) {
				System.out.println("Unknown tag kind in " + getClass().getName() + ":" + tag.getKind());
			}
			String tagText = tag.accept(new SimpleDocTreeVisitor<String, Integer>() {
            	public String visitUnknownBlockTag(UnknownBlockTagTree btt, Integer p) {
            		StringBuilder unStr = new StringBuilder();
            		for(DocTree childTag : btt.getContent()) {
            			if(childTag.getKind() != Kind.TEXT && childTag.getKind() != Kind.START_ELEMENT && childTag.getKind() != Kind.END_ELEMENT && childTag.getKind() != Kind.ENTITY) {
            				System.out.println("Unknown child tag kind in " + getClass().getName() + ":" + childTag.getKind());
            			}
            			unStr.append(childTag.accept(new SimpleDocTreeVisitor<String, Integer>(){
            				@Override
            				public String visitText(TextTree node, Integer p) {
            					return node.getBody();
            				}
            				@Override
            				public String visitStartElement(StartElementTree node, Integer p) {
            					StringBuilder startNodeStr = new StringBuilder();
            					startNodeStr.append("<");
            					startNodeStr.append(node.getName());
            					for(DocTree attr : node.getAttributes()) {
            						startNodeStr.append(attr.accept(new SimpleDocTreeVisitor<String, Integer>(){
										@Override
										public String visitAttribute(AttributeTree at, Integer p) {
			            					StringBuilder attrStr = new StringBuilder();
			            					attrStr.append(" ");
			            					attrStr.append(at.getName());
			            					attrStr.append("=\"");
			            					for(DocTree atvl : at.getValue()) {
			            						attrStr.append(atvl.accept(new SimpleDocTreeVisitor<String, Integer>(){
													@Override
													public String visitText(TextTree vtn, Integer p) {
														return vtn.getBody();
													}
			            							
			            						}, 0));
			            					}
			            					attrStr.append("\"");
											return attrStr.toString();
										}
            							
            						}, 0));
            					}
            					startNodeStr.append(">");
            					return  startNodeStr.toString();
            				}
							@Override
							public String visitEndElement(EndElementTree node, Integer p) {
            					return "</" + node.getName() + ">";
							}
							@Override
							public String visitEntity(EntityTree entity, Integer p) {
								return "&" + entity.getName().toString() + ";";
							}
							
            			}, 0));
            		}
            		return unStr.toString();
            	}
            }, 0);
        	
			if(tagText.contains("-")) { 
				String[] parts = tagText.split("-");
				buf.append("<b>");
				buf.append(parts[0]);
				buf.append("</b>");
				StringBuilder desc = new StringBuilder();
				for(int i = 1; i < parts.length; i++) { 
					desc.append(parts[i]);
				}
				String paramDesc = desc.toString();
				buf.append(paramDesc);

				BPLActionDetails.addParamDesc(parts[0], paramDesc);
			} else { 
				buf.append(tagText);
			}

            buf.append("</li>");
        }

        return buf.toString();
	}    
}

