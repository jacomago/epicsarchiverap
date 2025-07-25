/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.utils.ui;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.retrieval.mimeresponses.MimeResponse;
import org.json.simple.JSONArray;
import org.json.simple.JSONAware;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletResponse;

/**
 * Small utility for getting the contents of an URL as various things
 * @author mshankar
 *
 */
public class GetUrlContent {
    public static final String ARCHAPPL_COMPONENT = "ARCHAPPL_COMPONENT";
    private static final Logger logger = LogManager.getLogger(GetUrlContent.class);

    private GetUrlContent() {}

    /*
     * Given a URL and some query parameters, generate a query string
     */
    public static String generateURLWithQueryString(String url, Map<String, String> queryParameters) {
        StringWriter buf = new StringWriter();
        buf.append(url);
        boolean firstArg = true;
        Charset utf8 = Charset.forName("utf-8");
        for (String key : queryParameters.keySet()) {
            buf.append(firstArg ? "?" : "&");
            firstArg = false;
            buf.append(URLEncoder.encode(key, utf8));
            buf.append("=");
            buf.append(URLEncoder.encode(queryParameters.get(key), utf8));
        }
        return buf.toString();
    }

    /**
     * Given an URL and some query parameters, get the content of the URL
     * @param url URL
     * @param queryParameters Map
     * @return URL content as a JSON Object
     */
    public static JSONAware getURLContentWithQueryParameters(
            String url, Map<String, String> queryParameters, boolean logErrors) {
        try {
            String urlStr = generateURLWithQueryString(url, queryParameters);
            logger.debug("Getting the contents of " + urlStr + " as a JSON array");
            JSONParser parser = new JSONParser();
            try (InputStream is = getURLContentAsStream(urlStr)) {
                return (JSONAware) parser.parse(new InputStreamReader(is));
            }
        } catch (IOException ex) {
            if (logErrors) {
                logger.error("Exception getting contents of internal URL {}", url, ex);
            }
        } catch (ParseException pex) {
            if (logErrors) {
                logger.error(
                        "Parse exception getting contents of internal URL " + url + " at " + pex.getPosition(), pex);
            }
        }
        return null;
    }

    public static JSONObject getURLContentWithQueryParametersAsJSONObject(
            String url, Map<String, String> queryParameters, boolean logErrors) {
        return (JSONObject) getURLContentWithQueryParameters(url, queryParameters, logErrors);
    }

    public static JSONArray getURLContentWithQueryParametersAsJSONArray(
            String url, Map<String, String> queryParameters, boolean logErrors) {
        return (JSONArray) getURLContentWithQueryParameters(url, queryParameters, logErrors);
    }

    public static JSONObject getURLContentWithQueryParametersAsJSONObject(
            String url, Map<String, String> queryParameters) {
        return (JSONObject) getURLContentWithQueryParameters(url, queryParameters, true);
    }

    public static JSONArray getURLContentWithQueryParametersAsJSONArray(
            String url, Map<String, String> queryParameters) {
        return (JSONArray) getURLContentWithQueryParameters(url, queryParameters, true);
    }

    public static JSONArray getURLContentAsJSONArray(String urlStr) {
        return getURLContentWithQueryParametersAsJSONArray(urlStr, new HashMap<String, String>(), true);
    }

    public static JSONArray getURLContentAsJSONArray(String urlStr, boolean logErrors) {
        return getURLContentWithQueryParametersAsJSONArray(urlStr, new HashMap<String, String>(), logErrors);
    }

    public static JSONObject getURLContentAsJSONObject(String urlStr) {
        return getURLContentWithQueryParametersAsJSONObject(urlStr, new HashMap<String, String>(), true);
    }

    public static JSONObject getURLContentAsJSONObject(String urlStr, boolean logErrors) {
        return getURLContentWithQueryParametersAsJSONObject(urlStr, new HashMap<String, String>(), logErrors);
    }

    /**
     * Combine JSON arrays from multiple URL's in sequence and return a JSON Array.
     * We need the supress warnings here as JSONArray is a raw collection.
     *
     * @param urlStrs multiple URLs
     * @return Combined JSON arrays
     */
    @SuppressWarnings("unchecked")
    public static JSONArray combineJSONArrays(List<String> urlStrs) {
        JSONArray result = new JSONArray();
        for (String urlStr : urlStrs) {
            try {
                logger.debug("Getting the contents of " + urlStr + " as a JSON array.");
                JSONParser parser = new JSONParser();
                try (InputStream is = getURLContentAsStream(urlStr)) {
                    JSONArray content = (JSONArray) parser.parse(new InputStreamReader(is));
                    if (content != null) {
                        result.addAll(content);
                    } else {
                        logger.debug(urlStr + " returned an empty array");
                    }
                }
            } catch (IOException ex) {
                logger.error("Exception getting contents of internal URL " + urlStr, ex);
            } catch (ParseException pex) {
                logger.error(
                        "Parse exception getting contents of internal URL " + urlStr + " at " + pex.getPosition(), pex);
            }
        }
        return result;
    }

    /**
     * Combine JSON arrays of JSON objects from multiple URL's in sequence and sends them to the writer..
     * The difference from combineJSONArrays is that inserts a newline after each element.
     *
     * @param urlStrs multiple URLs
     * @param out PrintWriter
     */
    public static void combineJSONArraysAndPrintln(List<String> urlStrs, PrintWriter out) {
        out.println("[");
        boolean first = true;
        for (String urlStr : urlStrs) {
            try {
                logger.debug("Getting the contents of " + urlStr + " as a JSON array.");
                JSONParser parser = new JSONParser();
                try (InputStream is = getURLContentAsStream(urlStr)) {
                    JSONArray content = (JSONArray) parser.parse(new InputStreamReader(is));
                    if (content != null) {
                        for (Object obj : content) {
                            JSONObject jsonObj = (JSONObject) obj;
                            if (first) {
                                first = false;
                            } else {
                                out.println(",");
                            }
                            out.print(JSONValue.toJSONString(jsonObj));
                        }
                    } else {
                        logger.debug(urlStr + " returned an empty array");
                    }
                }
            } catch (IOException ex) {
                logger.error("Exception getting contents of internal URL " + urlStr, ex);
            } catch (ParseException pex) {
                logger.error(
                        "Parse exception getting contents of internal URL " + urlStr + " at " + pex.getPosition(), pex);
            }
        }
        out.println("]");
    }

    /**
     * A static utilty method to combine JSON objects
     * @param dest Details from additionalDetails are added to this.
     * @param additionalDetails JSONObject
     */
    @SuppressWarnings("unchecked")
    public static void combineJSONObjects(Map<String, String> dest, JSONObject additionalDetails) {
        if (additionalDetails != null) dest.putAll(additionalDetails);
    }

    /**
     * A static utilty method to combine JSON objects
     * @param dest Details from additionalDetails are added to this.
     * @param additionalDetails JSONArray
     */
    @SuppressWarnings("unchecked")
    public static void combineJSONArrays(List<Map<String, String>> dest, JSONArray additionalDetails) {
        if (additionalDetails != null) dest.addAll(additionalDetails);
    }

    @SuppressWarnings("unchecked")
    public static void combineJSONObjectsWithArrays(Map<String, Object> dest, JSONObject additionalDetails) {
        if (additionalDetails != null) dest.putAll(additionalDetails);
    }

    /*
     * Post a JSON object/array and get the results as JSON
     */
    public static JSONAware postDataAndGetContent(String url, JSONAware postBody) throws IOException {
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpPost postMethod = new HttpPost(url);
            postMethod.addHeader(ARCHAPPL_COMPONENT, "true");
            postMethod.addHeader("Content-Type", MimeTypeConstants.APPLICATION_JSON);
            postMethod.addHeader(
                    "Connection",
                    "close"); // https://www.nuxeo.com/blog/using-httpclient-properly-avoid-closewait-tcp-connections/
            StringEntity archiverValues =
                    new StringEntity(JSONValue.toJSONString(postBody), ContentType.APPLICATION_JSON);
            postMethod.setEntity(archiverValues);
            if (logger.isDebugEnabled()) {
                logger.debug("About to make a POST with " + url);
            }
            HttpResponse response = httpclient.execute(postMethod);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                logger.debug("Obtained a HTTP entity of length {}", entity.getContentLength());
                // ArchiverValuesHandler takes over the burden of closing the input stream.
                try (InputStream is = entity.getContent()) {
                    return (JSONAware) JSONValue.parse(new InputStreamReader(is));
                }
            } else {
                throw new IOException("HTTP response did not have an entity associated with it");
            }
        }
    }

    public static JSONObject postDataAndGetContentAsJSONObject(String url, JSONAware postBody) throws IOException {
        return (JSONObject) postDataAndGetContent(url, postBody);
    }

    public static JSONArray postDataAndGetContentAsJSONArray(String url, JSONAware postBody) throws IOException {
        return (JSONArray) postDataAndGetContent(url, postBody);
    }

    @SuppressWarnings("unchecked")
    public static JSONAware from(List<JSONObject> objs) {
        JSONArray ret = new JSONArray();
        ret.addAll(objs);
        return ret;
    }

    /**
     * Post a list of strings to the remove server as a CSV and return the results as a array of JSONObjects
     * @param url URL
     * @param paramName  &emsp;
     * @param params a list of strings
     * @return JSONArray  &emsp;
     * @throws IOException  &emsp;
     */
    public static <T> T postStringListAndGetJSON(String url, String paramName, Collection<String> params)
            throws IOException {
        StringWriter buf = new StringWriter();
        buf.append(paramName);
        buf.append("=");
        boolean isFirst = true;
        for (String param : params) {
            if (isFirst) {
                isFirst = false;
            } else {
                buf.append(",");
            }
            buf.append(param);
        }

        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpPost postMethod = new HttpPost(url);
            postMethod.addHeader("Content-Type", MimeTypeConstants.APPLICATION_FORM_URLENCODED);
            postMethod.addHeader(
                    "Connection",
                    "close"); // https://www.nuxeo.com/blog/using-httpclient-properly-avoid-closewait-tcp-connections/
            StringEntity archiverValues = new StringEntity(buf.toString(), ContentType.APPLICATION_FORM_URLENCODED);
            postMethod.setEntity(archiverValues);
            if (logger.isDebugEnabled()) {
                logger.debug("About to make a POST with " + url);
            }
            HttpResponse response = httpclient.execute(postMethod);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                logger.debug("Obtained a HTTP entity of length " + entity.getContentLength());
                // ArchiverValuesHandler takes over the burden of closing the input stream.
                try (InputStream is = entity.getContent()) {
                    @SuppressWarnings("unchecked")
                    T retval = (T) JSONValue.parse(new InputStreamReader(is));
                    return retval;
                }
            } else {
                throw new IOException("HTTP response did not have an entity associated with it");
            }
        }
    }

    /**
     * Check if we get a valid response from this URL
     *
     * @param urlStr URL
     */
    public static void checkURL(String urlStr) {
        try {
            logger.debug("Testing if " + urlStr + " is valid");
            try (InputStream ignored = getURLContentAsStream(urlStr)) {}
        } catch (IOException ex) {
            // Ignore any exceptions here as we are only testing if this is a valid URL.
        }
    }

    public static InputStream getURLContentAsStream(String serverURL) throws IOException {
        return getURLContentAsStream(serverURL, true);
    }

    public static InputStream getURLContentAsStream(String serverURL, boolean redirect) throws IOException {
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpGet getMethod = new HttpGet(serverURL);
            getMethod.addHeader("Connection", "close");
            if (!redirect) getMethod.addHeader("redirect", "false");
            getMethod.addHeader(ARCHAPPL_COMPONENT, "true");
            HttpResponse response = httpclient.execute(getMethod);
            if (response.getStatusLine().getStatusCode() == 200) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    logger.debug("Obtained a HTTP entity of length {}", entity.getContentLength());
                    // ArchiverValuesHandler takes over the burden of closing the input stream.
                    return new ByteArrayInputStream(entity.getContent().readAllBytes());

                } else {
                    throw new IOException("HTTP response did not have an entity associated with it");
                }
            } else {
                throw new IOException("Invalid status calling " + serverURL + ". Got "
                        + response.getStatusLine().getStatusCode() + " "
                        + response.getStatusLine().getReasonPhrase()
                        + response.getEntity().getContent());
            }
        }
    }

    /**
     * Get the contents of a redirect URL and use as reponse for the provided HttpServletResponse.
     * If possible, pass in error responses as well.
     * @param redirectURIStr  &emsp;
     * @param resp  HttpServletResponse
     * @throws IOException  &emsp;
     */
    public static void proxyURL(String redirectURIStr, HttpServletResponse resp) throws IOException {
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpGet getMethod = new HttpGet(redirectURIStr);
            getMethod.addHeader(
                    "Connection",
                    "close"); // https://www.nuxeo.com/blog/using-httpclient-properly-avoid-closewait-tcp-connections/
            try (CloseableHttpResponse response = httpclient.execute(getMethod)) {
                if (response.getStatusLine().getStatusCode() == 200) {
                    HttpEntity entity = response.getEntity();

                    HashSet<String> proxiedHeaders = new HashSet<>(Arrays.asList(MimeResponse.PROXIED_HEADERS));
                    Header[] headers = response.getAllHeaders();
                    for (Header header : headers) {
                        if (proxiedHeaders.contains(header.getName())) {
                            logger.debug("Adding headerName " + header.getName() + " and value " + header.getValue()
                                    + " when proxying request");
                            resp.addHeader(header.getName(), header.getValue());
                        }
                    }

                    if (entity != null) {
                        logger.debug("Obtained a HTTP entity of length " + entity.getContentLength());
                        try (OutputStream os = resp.getOutputStream();
                                InputStream is = new BufferedInputStream(entity.getContent())) {
                            byte[] buf = new byte[10 * 1024];
                            int bytesRead = is.read(buf);
                            while (bytesRead > 0) {
                                os.write(buf, 0, bytesRead);
                                resp.flushBuffer();
                                bytesRead = is.read(buf);
                            }
                        }
                    } else {
                        throw new IOException("HTTP response did not have an entity associated with it");
                    }
                } else {
                    logger.error(
                            "Invalid status code " + response.getStatusLine().getStatusCode()
                                    + " when connecting to URL " + redirectURIStr + ". Sending the errorstream across");
                    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                        try (InputStream is =
                                new BufferedInputStream(response.getEntity().getContent())) {
                            byte[] buf = new byte[10 * 1024];
                            int bytesRead = is.read(buf);
                            while (bytesRead > 0) {
                                os.write(buf, 0, bytesRead);
                                bytesRead = is.read(buf);
                            }
                        }
                        resp.sendError(response.getStatusLine().getStatusCode(), os.toString());
                    }
                }
            }
        }
    }
}
