package com.aub;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.JSONArray;
import org.json.JSONObject;

public class EmbeddedRegistry {

    private static final int STOP_SECONDS_DELAY = 1;
    private static final Pattern SUBJECT_VERSION_PATTERN = Pattern.compile("/subjects/(.*)/versions");
    private static final Pattern SUBJECT_LIST_PATTERN = Pattern.compile("/subjects$");

    private final int port;
    private HttpServer server;

    private Set<String> registeredSubjects = new HashSet<>();

    public EmbeddedRegistry(int port) {
        this.port = port;
    }

    public void start() {
        try {
            this.server = HttpServer.create(new InetSocketAddress(port), 0);

            this.server.createContext("/", httpExchange ->
            {
                String requestURI = httpExchange.getRequestURI().toString();
                Matcher subjectVersionMatcher = SUBJECT_VERSION_PATTERN.matcher(requestURI);

                if (subjectVersionMatcher.matches()) {
                    String subject = subjectVersionMatcher.group(1);
                    registeredSubjects.add(subject);

                    JSONObject uniqueID = new JSONObject();

                    // todo remove fixed id
                    uniqueID.put("id", 1);

                    byte response[] = uniqueID.toString().getBytes(StandardCharsets.UTF_8);
                    httpExchange.sendResponseHeaders(200, response.length);
                    OutputStream out = httpExchange.getResponseBody();
                    out.write(response);
                    out.close();
                }
                if (SUBJECT_LIST_PATTERN.matcher(requestURI).matches()) {
                    JSONArray array = new JSONArray();
                    registeredSubjects.forEach(array::put);
                    String responseString = array.toString();
                    byte response[] = responseString.getBytes(StandardCharsets.UTF_8);
                    httpExchange.sendResponseHeaders(200, response.length);
                    OutputStream out = httpExchange.getResponseBody();
                    out.write(response);
                    out.close();
                } else {
                    httpExchange.sendResponseHeaders(200, 0);
                    OutputStream out = httpExchange.getResponseBody();
                    out.close();
                }
            });

            this.server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        this.server.stop(STOP_SECONDS_DELAY);
    }
}
