package org.epics.archiverappliance.common.taglets;

import org.jspecify.annotations.NonNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;

/**
 * Called as part of the javadoc task;
 * This loads the javadoc generated file docs/api/mgmt_scriptables.txt and uses it to generate the docs/api/mgmt_scriptables.html document.
 * The docs/api/mgmt_scriptables.html contains a list of all the BPL's that can be accessed from outside the system (perhaps thru python/bash)
 * @author mshankar
 *
 */
public class ProcessMgmtScriptables {
    static class BPLParam {
        final String paramName;
        final String paramDesc;

        public BPLParam(List<String> lines) {
            this.paramName = lines.get(0);
            this.paramDesc = lines.get(1);
        }
    }

    static class BPLActionDetail {
        final String path;
        final String bplclass;
        final String actiondesc;
        final LinkedList<BPLParam> paramDesc = new LinkedList<BPLParam>();

        public BPLActionDetail(List<String> lines) {
            this.path = lines.get(0);
            this.bplclass = lines.get(1);
            this.actiondesc = lines.get(2);
        }
    }
    /**
     * @param args  &emsp;
     * @throws Exception  &emsp;
     */
    public static void main(String[] args) throws Exception {
       File mgmtScriptablesFile = new File(args[0]);

       if (!mgmtScriptablesFile.exists()) {
           throw new RuntimeException("mgmt scriptables file " + mgmtScriptablesFile + " does not exist");
       }

       File mgmtPathMappingsFile = new File(args[1]);

       if (!mgmtPathMappingsFile.exists()) {
           throw new RuntimeException("mgmt pathings file " + mgmtPathMappingsFile + " does not exist");
       }

       File templateFile = new File("docs/templates/mgmt_scriptables_template.html");
        //		@StartMethod
        //		/getPVStatus
        //		org.epics.archiverappliance.mgmt.bpl.GetPVStatusAction
        //		- Get the status of a PV.
        //		@MethodDescDone
        //		@StartParam
        //		pv
        //		 The name of the pv for which status is to be determined.
        //		@EndParam
        //		@EndMethod

        List<BPLActionDetail> actionDetails = new LinkedList<>();
        try (LineNumberReader in = new LineNumberReader(
                new InputStreamReader(new FileInputStream(mgmtScriptablesFile)))) {
            actionDetails = parseFile(in);
        }

        final LinkedList<String> pathsInBPLServletSequence = parseMgmtPathMappingsFile(mgmtPathMappingsFile);

        // Now do the sort.
        actionDetails.sort((o1, o2) -> {
            int posn1 = pathsInBPLServletSequence.indexOf(o1.path);
            int posn2 = pathsInBPLServletSequence.indexOf(o2.path);
            return posn1 - posn2;
        });

        writeHtmlFile(templateFile, actionDetails);
    }

    private static void writeHtmlFile(File templateFile, List<BPLActionDetail> actionDetails) throws IOException {

        // We get the template and replace the @Content tag with the generated content.
        try (LineNumberReader in = new LineNumberReader(new InputStreamReader(
                        new FileInputStream(templateFile)));
                PrintWriter out =
                        new PrintWriter(System.out)) {
            // Copy the template till we come to @Content
            String line = in.readLine();
            while (line != null) {
                if (line.startsWith("@Content")) {
                    break;
                }
                out.println(line);
                line = in.readLine();
            }

            for (BPLActionDetail actionDetail : actionDetails) {
                out.println("<div class=\"bplelement\"><h1>");
                out.println(actionDetail.path);
                out.println("</h1><div>");
                out.println(actionDetail.actiondesc);
                out.print("</div><br/>For more details, please see the <a href=\"../_static/javadoc/");
                out.print(actionDetail.bplclass.replace('.', '/'));
                out.println(".html\">javadoc</a><div><dl>");
                for (BPLParam param : actionDetail.paramDesc) {
                    out.println("<dt>");
                    out.println(param.paramName);
                    out.println("</dt>");
                    out.println("<dd>");
                    out.println(param.paramDesc);
                    out.println("</dd>");
                }
                out.println("</dl></div>");
                out.println("</div>");
            }

            // Copy the rest of the template
            line = in.readLine();
            while (line != null) {
                out.println(line);
                line = in.readLine();
            }
        }
    }

    private static @NonNull LinkedList<String> parseMgmtPathMappingsFile(File mgmtPathMappingsFile) throws IOException {
        // We want to sort actionDetails according to the location in the BPLServlet.
        // This is output to mgmtpathmappings.txt as part of the javadoc ant task.
        // We read the sequence from there. Here's a sample

        //    	#Path mappings for mgmt BPLs
        //    	#Tue Oct 16 18:10:26 PDT 2012
        //    	/resumeArchivingPV=org.epics.archiverappliance.mgmt.bpl.ResumeArchivingPV
        final LinkedList<String> pathsInBPLServletSequence = new LinkedList<>();
        try (LineNumberReader in = new LineNumberReader(
                new InputStreamReader(new FileInputStream(mgmtPathMappingsFile)))) {
            String line = in.readLine();
            while (line != null) {
                if (!line.startsWith("#") && line.contains("=")) {
                    String[] parts = line.split("=");
                    String path = parts[0];
                    pathsInBPLServletSequence.add(path);
                }
                line = in.readLine();
            }
        }
        return pathsInBPLServletSequence;
    }

    private static List<BPLActionDetail> parseFile(LineNumberReader in) throws IOException {
        List<String> lines = new LinkedList<>();
        BPLActionDetail currentAction = null;
        List<BPLActionDetail> actionDetails = new LinkedList<>();

        String line = in.readLine();
        while (line != null) {
            switch (line) {
                case "@StartMethod": {
                    currentAction = null;
                    lines = new LinkedList<>();
                    break;
                }
                case "@MethodDescDone": {
                    currentAction = new BPLActionDetail(lines);
                    lines = new LinkedList<>();
                    break;
                }
                case "@StartParam": {
                    lines = new LinkedList<>();
                    break;
                }
                case "@EndParam": {
                    if (currentAction == null) {
                        throw new RuntimeException("Found @EndParam without @StartMethod");
                    }
                    currentAction.paramDesc.add(new BPLParam(lines));
                    lines = new LinkedList<>();
                    break;
                }
                case "@EndMethod": {
                    actionDetails.add(currentAction);
                    currentAction = null;
                    lines = new LinkedList<>();
                    break;
                }
                default: {
                    lines.add(line);
                    break;
                }
            }
            line = in.readLine();
        }
        return actionDetails;
    }
}
