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
	private static File scriptablesFile;

	// Javadoc loads each taglet with a separate class loader, so static state
	// set by BPLActionTaglet.init() is not visible to BPLActionParamTaglet or
	// BPLActionEndTaglet. Initialize here so every copy has a usable default.
	// The javadoc process CWD is the subproject directory (mgmt-service/).
	static {
		scriptablesFile = new File("build/tmp/mgmt_scriptables.txt");
	}

	public static void setScriptablesFile(File file) {
		scriptablesFile = file;
	}

	public static void addMethod(String path, String bplclassName, String actionDescription) {
    	try(PrintWriter out = new PrintWriter(new FileOutputStream(scriptablesFile, true))) {
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
    	try(PrintWriter out = new PrintWriter(new FileOutputStream(scriptablesFile, true))) {
    		out.println("@StartParam");
    		out.println(paramName);
    		out.println(paramDesc);
    		out.println("@EndParam");
    	} catch(Exception ex) { 
    		throw new RuntimeException(ex);
    	}
	}
    
	public static void addMethodTerminator() {
    	try(PrintWriter out = new PrintWriter(new FileOutputStream(scriptablesFile, true))) {
    		out.println("@EndMethod");
    	} catch(Exception ex) { 
    		throw new RuntimeException(ex);
    	}
	}
}
