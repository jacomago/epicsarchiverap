package org.epics.archiverappliance.taglets;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;

/**
 * List of BPLActionDetail's; we send this to a file called mgmt_scriptables.txt.
 * 
 * @author mshankar
 *
 */
public class BPLActionDetails {
	
	public static void addMethod(String path, String bplclassName, String actionDescription) {
    	try(PrintWriter out = new PrintWriter(new FileOutputStream(new File("docs/api/mgmt_scriptables.txt"), true))) {
    		out.println("@StartMethod");
    		out.println(path);
    		out.println(bplclassName);
    		out.println(actionDescription);
    		out.println("@MethodDescDone");
    	} catch(Exception ex) { 
    		throw new RuntimeException(ex);
    	}
	}

    public static void addParamDesc(String paramName, String paramDesc) { 
    	try(PrintWriter out = new PrintWriter(new FileOutputStream(new File("docs/api/mgmt_scriptables.txt"), true))) {
    		out.println("@StartParam");
    		out.println(paramName);
    		out.println(paramDesc);
    		out.println("@EndParam");
    	} catch(Exception ex) { 
    		throw new RuntimeException(ex);
    	}
	}
    
	public static void addMethodTerminator() {
    	try(PrintWriter out = new PrintWriter(new FileOutputStream(new File("docs/api/mgmt_scriptables.txt"), true))) {
    		out.println("@EndMethod");
    	} catch(Exception ex) { 
    		throw new RuntimeException(ex);
    	}
	}
}
