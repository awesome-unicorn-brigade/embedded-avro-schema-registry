package com.aub;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.json.JSONArray;
import org.json.JSONObject;

public class EmbeddedRegistry {

    private static final int STOP_SECONDS_DELAY = 1;
    private static final Pattern SUBJECT_VERSION_PATTERN = Pattern.compile("/subjects/(.*)/versions");
    private static final Pattern SUBJECT_LIST_PATTERN = Pattern.compile("/subjects$");
    private static final Pattern SCHEMA_FETCH_PATTERN = Pattern.compile("/schemas/ids/(\\d+)$");

    private final int port;
    private HttpServer server;

    private Map<Integer, Schema> registeredSchemas = new HashMap<>();
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
                    int key = registeredSchemas.size() + 1;
                    
                    registeredSchemas.put(key, new Schema.Parser()
                        .parse(readBody(httpExchange)));
                    registeredSubjects.add(subject);
                    JSONObject uniqueID = new JSONObject();
                    uniqueID.put("id", key);

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
                }
                Matcher schemaFetchMatcher = SCHEMA_FETCH_PATTERN.matcher(requestURI);
                if (schemaFetchMatcher.matches()) {
                    int schemaId = Integer.parseInt(schemaFetchMatcher.group(1));
                    String schemaDefinition = registeredSchemas.get(schemaId).toString();
                    
                    JSONObject obj = new JSONObject();
                    obj.put("schema", schemaDefinition);
                    
                    byte response[] = obj.toString()
                        .getBytes(StandardCharsets.UTF_8);
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

    private String readBody(HttpExchange httpExchange) {
        return new BufferedReader(new InputStreamReader(httpExchange.getRequestBody()))
            .lines().collect(Collectors.joining("\n"));
    }

    public void stop() {
        this.server.stop(STOP_SECONDS_DELAY);
    }
}
