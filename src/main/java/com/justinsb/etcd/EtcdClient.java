package com.justinsb.etcd;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.net.MediaType;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.mastfrog.acteur.headers.Method;
import com.mastfrog.netty.http.client.HttpClient;
import com.mastfrog.netty.http.client.HttpRequestBuilder;
import com.mastfrog.netty.http.client.ResponseFuture;
import com.mastfrog.netty.http.client.ResponseHandler;
import com.mastfrog.url.Parameters;
import com.mastfrog.url.ParametersElement;
import com.mastfrog.util.AbstractBuilder;
import com.mastfrog.util.Exceptions;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class EtcdClient {

    static final Gson gson = new GsonBuilder().create();

    final URI baseUri;
    private final HttpClient httpClient;

    public EtcdClient(URI baseUri) {
        this(baseUri, HttpClient.builder()
                .maxChunkSize(512)
                .noCompression()
                .maxInitialLineLength(255)
                .followRedirects()
                .threadCount(1)
                .dontSend100Continue()
                .build());
    }

    public EtcdClient(URI baseUri, HttpClient client) {
        this.httpClient = client;
        String uri = baseUri.toString();
        if (!uri.endsWith("/")) {
            uri += "/";
            baseUri = URI.create(uri);
        }
        this.baseUri = baseUri;
    }

    /**
     * Retrieves a key. Returns null if not found.
     */
    public EtcdResult get(String key) throws EtcdClientException {
        URI uri = buildKeyUri("v2/keys", key, "");
        UriRequest request = new UriRequest(Method.GET, uri);

        EtcdResult result = syncExecute(request, new int[]{200, 404}, 100);
        if (result.isError()) {
            if (result.errorCode == 100) {
                return null;
            }
        }
        return result;
    }

    /**
     * Deletes the given key
     */
    public EtcdResult delete(String key) throws EtcdClientException {
        URI uri = buildKeyUri("v2/keys", key, "");
        UriRequest request = new UriRequest(Method.DELETE, uri);

        return syncExecute(request, new int[]{200, 404});
    }

    /**
     * Sets a key to a new value
     */
    public EtcdResult set(String key, String value) throws EtcdClientException {
        return set(key, value, null);
    }

    /**
     * Sets a key to a new value with an (optional) ttl
     */
    public EtcdResult set(String key, String value, Integer ttl) throws EtcdClientException {
        List<BasicNameValuePair> data = Lists.newArrayList();
        data.add(new BasicNameValuePair("value", value));
        if (ttl != null) {
            data.add(new BasicNameValuePair("ttl", Integer.toString(ttl)));
        }

        return set0(key, data, new int[]{200, 201});
    }

    /**
     * Creates a directory
     */
    public EtcdResult createDirectory(String key) throws EtcdClientException {
        List<BasicNameValuePair> data = Lists.newArrayList();
        data.add(new BasicNameValuePair("dir", "true"));
        return set0(key, data, new int[]{200, 201});
    }

    /**
     * Lists a directory
     */
    public List<EtcdNode> listDirectory(String key) throws EtcdClientException {
        EtcdResult result = get(key + "/");
        if (result == null || result.node == null) {
            return null;
        }
        return result.node.nodes;
    }

    /**
     * Delete a directory
     */
    public EtcdResult deleteDirectory(String key) throws EtcdClientException {
        URI uri = buildKeyUri("v2/keys", key, "?dir=true");
        UriRequest request = new UriRequest(Method.DELETE, uri);
        return syncExecute(request, new int[]{202});
    }

    /**
     * Sets a key to a new value, if the value is a specified value
     */
    public EtcdResult cas(String key, String prevValue, String value) throws EtcdClientException {
        List<BasicNameValuePair> data = Lists.newArrayList();
        data.add(new BasicNameValuePair("value", value));
        data.add(new BasicNameValuePair("prevValue", prevValue));

        return set0(key, data, new int[]{200, 412}, 101);
    }

    /**
     * Watches the given subtree
     */
    public ListenableFuture<EtcdResult> watch(String key) throws EtcdClientException {
        return watch(key, null, false);
    }

    /**
     * Watches the given subtree
     */
    public ListenableFuture<EtcdResult> watch(String key, Long index, boolean recursive) throws EtcdClientException {
        String suffix = "?wait=true";
        if (index != null) {
            suffix += "&waitIndex=" + index;
        }
        if (recursive) {
            suffix += "&recursive=true";
        }
        URI uri = buildKeyUri("v2/keys", key, suffix);

        UriRequest request = new UriRequest(Method.GET, uri);

        return asyncExecute(request, new int[]{200});
    }

    /**
     * Gets the etcd version
     */
    public String getVersion() throws EtcdClientException {
        URI uri = baseUri.resolve("version");

        UriRequest request = new UriRequest(Method.GET, uri);

        // Technically not JSON, but it'll work
        // This call is the odd one out
        JsonResponse s = syncExecuteJson(request, 200);
        if (s.httpStatusCode != 200) {
            throw new EtcdClientException("Error while fetching versions", s.httpStatusCode);
        }
        return s.json;
    }

    static class BasicNameValuePair {

        private final String key;
        private final String value;

        public BasicNameValuePair(String key, String value) {
            this.key = key;
            this.value = value;
        }

    }

    private String encodePairs(List<BasicNameValuePair> pairs) {
        AbstractBuilder<ParametersElement, Parameters> b = com.mastfrog.url.Parameters.builder();
        StringBuilder sb = new StringBuilder();
        for (BasicNameValuePair p : pairs) {
            try {
                if (sb.length() > 0) {
                    sb.append('&');
                }
                sb.append(URLEncoder.encode(p.key, "UTF-8")).append('=').append(URLEncoder.encode(p.value, "UTF-8"));
            } catch (UnsupportedEncodingException ex) {
                Exceptions.chuck(ex);
            }
        }
        return sb.toString();
    }

    private EtcdResult set0(String key, List<BasicNameValuePair> data, int[] httpErrorCodes, int... expectedErrorCodes)
            throws EtcdClientException {
        URI uri = buildKeyUri("v2/keys", key, "");
        String body = encodePairs(data);
        UriRequest request = new UriRequest(Method.PUT, uri, body);

        return syncExecute(request, httpErrorCodes, expectedErrorCodes);
    }

    public EtcdResult listChildren(String key) throws EtcdClientException {
        URI uri = buildKeyUri("v2/keys", key, "/");
        UriRequest request = new UriRequest(Method.GET, uri);

        EtcdResult result = syncExecute(request, new int[]{200});
        return result;
    }

    static class UriRequest {

        public final Method method;
        public final URI uri;
        public final String body;

        public UriRequest(Method method, URI uri) {
            this(method, uri, null);
        }

        public UriRequest(Method method, URI uri, String body) {
            this.method = method;
            this.uri = uri;
            this.body = body;
        }

        public String toString() {
            return method.name() + ' ' + uri + (body == null ? "" : " --> " + body);
        }
    }

    protected ListenableFuture<EtcdResult> asyncExecute(UriRequest request, int[] expectedHttpStatusCodes, final int... expectedErrorCodes)
            throws EtcdClientException {
        ListenableFuture<JsonResponse> json = asyncExecuteJson(request, expectedHttpStatusCodes);
        return Futures.transform(json, new AsyncFunction<JsonResponse, EtcdResult>() {
            public ListenableFuture<EtcdResult> apply(JsonResponse json) throws Exception {
                EtcdResult result = jsonToEtcdResult(json, expectedErrorCodes);
                return Futures.immediateFuture(result);
            }
        });
    }

    protected EtcdResult syncExecute(UriRequest request, int[] expectedHttpStatusCodes, int... expectedErrorCodes) throws EtcdClientException {
        try {
            return asyncExecute(request, expectedHttpStatusCodes, expectedErrorCodes).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new EtcdClientException("Interrupted during request", e);
        } catch (ExecutionException e) {
            throw unwrap(e);
        }
        // String json = syncExecuteJson(request);
        // return jsonToEtcdResult(json, expectedErrorCodes);
    }

    private EtcdClientException unwrap(ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof EtcdClientException) {
            return (EtcdClientException) cause;
        }
        return new EtcdClientException("Error executing request", e);
    }

    private EtcdResult jsonToEtcdResult(JsonResponse response, int... expectedErrorCodes) throws EtcdClientException {
        if (response == null || response.json == null) {
            return null;
        }
        EtcdResult result = parseEtcdResult(response.json);

        if (result.isError()) {
            if (!contains(expectedErrorCodes, result.errorCode)) {
                throw new EtcdClientException(result.message, result);
            }
        }
        return result;
    }

    private EtcdResult parseEtcdResult(String json) throws EtcdClientException {
        EtcdResult result;
        try {
            result = gson.fromJson(json, EtcdResult.class);
        } catch (JsonParseException e) {
            throw new EtcdClientException("Error parsing response from etcd", e);
        }
        return result;
    }

    private static boolean contains(int[] list, int find) {
        for (int i = 0; i < list.length; i++) {
            if (list[i] == find) {
                return true;
            }
        }
        return false;
    }

    protected List<EtcdResult> syncExecuteList(UriRequest request) throws EtcdClientException {
        JsonResponse response = syncExecuteJson(request, 200);
        if (response.json == null) {
            return null;
        }

        if (response.httpStatusCode != 200) {
            EtcdResult etcdResult = parseEtcdResult(response.json);
            throw new EtcdClientException("Error listing keys", etcdResult);
        }

        try {
            List<EtcdResult> ret = new ArrayList<EtcdResult>();
            JsonParser parser = new JsonParser();
            JsonArray array = parser.parse(response.json).getAsJsonArray();
            for (int i = 0; i < array.size(); i++) {
                EtcdResult next = gson.fromJson(array.get(i), EtcdResult.class);
                ret.add(next);
            }
            return ret;
        } catch (JsonParseException e) {
            throw new EtcdClientException("Error parsing response from etcd", e);
        }
    }

    protected JsonResponse syncExecuteJson(UriRequest request, int... expectedHttpStatusCodes) throws EtcdClientException {
        try {
            return asyncExecuteJson(request, expectedHttpStatusCodes).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EtcdClientException("Interrupted during request processing", e);
        } catch (ExecutionException e) {
            throw unwrap(e);
        }
    }

    protected ListenableFuture<JsonResponse> asyncExecuteJson(UriRequest request, final int[] expectedHttpStatusCodes) throws EtcdClientException {
        ListenableFuture<Response> response = asyncExecuteHttp(request);

        return Futures.transform(response, new AsyncFunction<Response, JsonResponse>() {
            public ListenableFuture<JsonResponse> apply(Response httpResponse) throws Exception {
                JsonResponse json = extractJsonResponse(httpResponse, expectedHttpStatusCodes);
                return Futures.immediateFuture(json);
            }
        });
    }

    /**
     * We need the status code & the response to parse an error response.
     */
    static class JsonResponse {

        final String json;
        final int httpStatusCode;

        public JsonResponse(String json, int statusCode) {
            this.json = json;
            this.httpStatusCode = statusCode;
        }

    }

    protected JsonResponse extractJsonResponse(Response httpResponse, int[] expectedHttpStatusCodes) throws EtcdClientException {
        HttpResponseStatus statusLine = httpResponse.status;
        int statusCode = statusLine.code();

        String json = httpResponse.entity;

        if (!contains(expectedHttpStatusCodes, statusCode)) {
            if (statusCode == 400 && json != null) {
                // More information in JSON
            } else {
                throw new EtcdClientException("Error response from etcd: " + statusLine.toString(),
                        statusCode);
            }
        }

        return new JsonResponse(json, statusCode);
    }

    private URI buildKeyUri(String prefix, String key, String suffix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix);
        if (key.startsWith("/")) {
            key = key.substring(1);
        }
        for (String token : Splitter.on('/').split(key)) {
            sb.append("/");
            sb.append(urlEscape(token));
        }
        sb.append(suffix);

        URI uri = baseUri.resolve(sb.toString());
        return uri;
    }

    protected ListenableFuture<Response> asyncExecuteHttp(UriRequest request) {
        final SettableFuture<Response> future = SettableFuture.create();

        class Handler extends ResponseHandler<String> {

            public Handler() {
                super(String.class);
            }

            @Override
            protected void onError(Throwable err) {
                future.setException(err);
            }

            @Override
            protected void onErrorResponse(HttpResponseStatus status, HttpHeaders headers, String content) {
//                future.setException(new IOException(status + content));
                future.set(new Response(status, headers, content));
            }

            @Override
            protected void receive(HttpResponseStatus status, HttpHeaders headers, String content) {
                future.set(new Response(status, headers, content));
            }
        }

        HttpRequestBuilder bldr = httpClient.request(request.method).setURL(request.uri.toASCIIString());

        if (request.body != null) {
            try {
                bldr.setBody(request.body, MediaType.parse("application/x-www-form-urlencoded"));
            } catch (IOException ex) {
                Exceptions.chuck(ex);
            }
        }
        ResponseFuture fut = bldr.execute(new Handler());
        // Pending - could listen on the FullContentReceived state and respond
        // without waiting for the connection to be idle/closed
        return future;
    }

    static class Response {

        public final HttpResponseStatus status;
        public final HttpHeaders headers;
        public final String entity;

        public Response(HttpResponseStatus status, HttpHeaders headers, String entity) {
            this.status = status;
            this.headers = headers;
            this.entity = entity;
        }

    }

    protected static String urlEscape(String s) {
        try {
            return URLEncoder.encode(s, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException();
        }
    }

    public static String format(Object o) {
        try {
            return gson.toJson(o);
        } catch (Exception e) {
            return "Error formatting: " + e.getMessage();
        }
    }

}
