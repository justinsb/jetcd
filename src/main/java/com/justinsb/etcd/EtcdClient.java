package com.justinsb.etcd;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

public class EtcdClient {
  private static final Gson gson = new GsonBuilder().create();

  private final CloseableHttpAsyncClient httpClient;
  private final URI baseUri;

  static CloseableHttpAsyncClient buildDefaultHttpClient() {
    final RequestConfig requestConfig = RequestConfig.custom().build();
    final CloseableHttpAsyncClient httpClient = HttpAsyncClients.custom().setDefaultRequestConfig(requestConfig).build();
    httpClient.start();
    return httpClient;
  }

  public EtcdClient(final URI baseUri) {
    this(baseUri, buildDefaultHttpClient());
  }

  public EtcdClient(final URI baseUri, final CloseableHttpAsyncClient httpClient) {
    String uri = baseUri.toString();
    if (!uri.endsWith("/")) {
      uri += "/";
      this.baseUri = URI.create(uri);
    } else {
      this.baseUri = baseUri;
    }

    this.httpClient = httpClient;
  }

  /**
   * Retrieves a key. Returns null if not found.
   */
  public EtcdResult get(final String key) throws EtcdClientException {
    final URI uri = buildKeyUri("v2/keys", key, "");
    final HttpGet request = new HttpGet(uri);

    final EtcdResult result = syncExecute(request, new int[]{200, 404}, 100);
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
  public EtcdResult delete(final String key) throws EtcdClientException {
    final URI uri = buildKeyUri("v2/keys", key, "");
    final HttpDelete request = new HttpDelete(uri);

    return syncExecute(request, new int[]{200, 404});
  }

  /**
   * Sets a key to a new value
   */
  public EtcdResult set(final String key, final String value) throws EtcdClientException {
    return set(key, value, null);
  }

  /**
   * Sets a key to a new value with an (optional) ttl
   */

  public EtcdResult set(final String key, final String value, final Integer ttl) throws EtcdClientException {
    final List<BasicNameValuePair> data = Lists.newArrayList(new BasicNameValuePair("value", value));
    if (ttl != null) {
      data.add(new BasicNameValuePair("ttl", Integer.toString(ttl)));
    }

    return set0(key, data, new int[]{200, 201});
  }

  /**
   * Creates a directory
   */
  public EtcdResult createDirectory(final String key) throws EtcdClientException {
    final List<BasicNameValuePair> data = Lists.newArrayList(new BasicNameValuePair("dir", "true"));
    return set0(key, data, new int[]{200, 201});
  }

  /**
   * Lists a directory
   */
  public List<EtcdNode> listDirectory(final String key) throws EtcdClientException {
    final EtcdResult result = get(key + "/");
    if (result == null || result.node == null) {
      return null;
    }
    return result.node.nodes;
  }

  /**
   * Delete a directory
   */
  public EtcdResult deleteDirectory(final String key) throws EtcdClientException {
    final URI uri = buildKeyUri("v2/keys", key, "?dir=true");
    final HttpDelete request = new HttpDelete(uri);
    return syncExecute(request, new int[]{202});
  }

  /**
   * Sets a key to a new value, if the value is a specified value
   */
  public EtcdResult cas(final String key, final String prevValue, final String value) throws EtcdClientException {
    final List<BasicNameValuePair> data = Lists.newArrayList();
    data.add(new BasicNameValuePair("value", value));
    data.add(new BasicNameValuePair("prevValue", prevValue));

    return set0(key, data, new int[]{200, 412}, 101);
  }

  /**
   * Watches the given subtree
   */
  public ListenableFuture<EtcdResult> watch(final String key) throws EtcdClientException {
    return watch(key, null, false);
  }

  /**
   * Watches the given subtree
   */
  public ListenableFuture<EtcdResult> watch(final String key, final Long index, final boolean recursive) throws EtcdClientException {
    String suffix = "?wait=true";
    if (index != null) {
      suffix += "&waitIndex=" + index;
    }
    if (recursive) {
      suffix += "&recursive=true";
    }

    final URI uri = buildKeyUri("v2/keys", key, suffix);
    final HttpGet request = new HttpGet(uri);
    return asyncExecute(request, new int[]{200});
  }

  /**
   * Gets the etcd version
   */
  public String getVersion() throws EtcdClientException {
    final URI uri = baseUri.resolve("version");
    final HttpGet request = new HttpGet(uri);

    // Technically not JSON, but it'll work
    // This call is the odd one out
    final JsonResponse s = syncExecuteJson(request, 200);
    if (s.httpStatusCode != 200) {
      throw new EtcdClientException("Error while fetching versions",
          s.httpStatusCode);
    }
    return s.json;
  }

  private EtcdResult set0(final String key, final List<BasicNameValuePair> data, final int[] httpErrorCodes, final int... expectedErrorCodes) throws EtcdClientException {
    final URI uri = buildKeyUri("v2/keys", key, "");
    final HttpPut request = new HttpPut(uri);
    final UrlEncodedFormEntity entity = new UrlEncodedFormEntity(data, Charsets.UTF_8);
    request.setEntity(entity);

    return syncExecute(request, httpErrorCodes, expectedErrorCodes);
  }

  public EtcdResult listChildren(final String key) throws EtcdClientException {
    final URI uri = buildKeyUri("v2/keys", key, "/");
    final HttpGet request = new HttpGet(uri);
    return syncExecute(request, new int[]{200});
  }

  protected ListenableFuture<EtcdResult> asyncExecute(final HttpUriRequest request, final int[] expectedHttpStatusCodes, final int... expectedErrorCodes) throws EtcdClientException {
    final ListenableFuture<JsonResponse> json = asyncExecuteJson(request, expectedHttpStatusCodes);
    return Futures.transform(json,
        new AsyncFunction<JsonResponse, EtcdResult>() {
          public ListenableFuture<EtcdResult> apply(final JsonResponse json) throws Exception {
            final EtcdResult result = jsonToEtcdResult(json, expectedErrorCodes);
            return Futures.immediateFuture(result);
          }
        }
    );
  }

  protected EtcdResult syncExecute(final HttpUriRequest request, final int[] expectedHttpStatusCodes, final int... expectedErrorCodes) throws EtcdClientException {
    try {
      return asyncExecute(request, expectedHttpStatusCodes, expectedErrorCodes).get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();

      throw new EtcdClientException("Interrupted during request", e);
    } catch (final ExecutionException e) {
      throw unwrap(e);
    }
  }

  private EtcdClientException unwrap(final ExecutionException e) {
    final Throwable cause = e.getCause();
    if (cause instanceof EtcdClientException) {
      return (EtcdClientException) cause;
    }
    return new EtcdClientException("Error executing request", e);
  }

  private EtcdResult jsonToEtcdResult(final JsonResponse response, final int... expectedErrorCodes) throws EtcdClientException {
    if (response == null || response.json == null) {
      return null;
    }

    final EtcdResult result = parseEtcdResult(response.json);

    if (result.isError()) {
      if (!contains(expectedErrorCodes, result.errorCode)) {
        throw new EtcdClientException(result.message, result);
      }
    }
    return result;
  }

  private EtcdResult parseEtcdResult(final String json) throws EtcdClientException {
    try {
      return gson.fromJson(json, EtcdResult.class);
    } catch (JsonParseException e) {
      throw new EtcdClientException("Error parsing response from etcd", e);
    }
  }

  private static boolean contains(final int[] list, final int find) {
    for (final int listItem : list) {
      if (listItem == find) {
        return true;
      }
    }
    return false;
  }

  protected List<EtcdResult> syncExecuteList(final HttpUriRequest request) throws EtcdClientException {
    final JsonResponse response = syncExecuteJson(request, 200);
    if (response.json == null) {
      return null;
    }

    if (response.httpStatusCode != 200) {
      final EtcdResult etcdResult = parseEtcdResult(response.json);
      throw new EtcdClientException("Error listing keys", etcdResult);
    }

    try {
      final List<EtcdResult> ret = new ArrayList<EtcdResult>();
      final JsonParser parser = new JsonParser();
      final JsonArray array = parser.parse(response.json).getAsJsonArray();

      for (int i = 0; i < array.size(); i++) {
        final EtcdResult next = gson.fromJson(array.get(i), EtcdResult.class);
        ret.add(next);
      }

      return ret;
    } catch (final JsonParseException e) {
      throw new EtcdClientException("Error parsing response from etcd", e);
    }
  }

  protected JsonResponse syncExecuteJson(final HttpUriRequest request, final int... expectedHttpStatusCodes) throws EtcdClientException {
    try {
      return asyncExecuteJson(request, expectedHttpStatusCodes).get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new EtcdClientException("Interrupted during request processing", e);
    } catch (final ExecutionException e) {
      throw unwrap(e);
    }
  }

  protected ListenableFuture<JsonResponse> asyncExecuteJson(final HttpUriRequest request, final int[] expectedHttpStatusCodes) throws EtcdClientException {
    final ListenableFuture<HttpResponse> response = asyncExecuteHttp(request);

    return Futures.transform(response, new AsyncFunction<HttpResponse, JsonResponse>() {
      public ListenableFuture<JsonResponse> apply(final HttpResponse httpResponse) throws Exception {
        final JsonResponse json = extractJsonResponse(httpResponse, expectedHttpStatusCodes);
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

  protected JsonResponse extractJsonResponse(final HttpResponse httpResponse, final int[] expectedHttpStatusCodes) throws EtcdClientException {
    try {
      final StatusLine statusLine = httpResponse.getStatusLine();
      final int statusCode = statusLine.getStatusCode();

      String json = null;

      if (httpResponse.getEntity() != null) {
        try {
          json = EntityUtils.toString(httpResponse.getEntity());
        } catch (final IOException e) {
          throw new EtcdClientException("Error reading response", e);
        }
      }

      if (!contains(expectedHttpStatusCodes, statusCode)) {
        if (statusCode == 400 && json != null) {
          // More information in JSON
        } else {
          throw new EtcdClientException("Error response from etcd: " + statusLine.getReasonPhrase(), statusCode);
        }
      }

      return new JsonResponse(json, statusCode);
    } finally {
      close(httpResponse);
    }
  }

  private URI buildKeyUri(final String prefix, final String key, final String suffix) {
    String tKey = key;
    final StringBuilder sb = new StringBuilder();
    sb.append(prefix);
    if (tKey.startsWith("/")) {
      tKey = tKey.substring(1);
    }

    for (final String token : Splitter.on('/').split(tKey)) {
      sb.append("/");
      sb.append(urlEscape(token));
    }

    sb.append(suffix);

    return baseUri.resolve(sb.toString());
  }

  protected ListenableFuture<HttpResponse> asyncExecuteHttp(final HttpUriRequest request) {
    final SettableFuture<HttpResponse> future = SettableFuture.create();

    httpClient.execute(request, new FutureCallback<HttpResponse>() {
      @Override
      public void completed(final HttpResponse result) {
        future.set(result);
      }

      @Override
      public void failed(final Exception ex) {
        future.setException(ex);
      }

      @Override
      public void cancelled() {
        future.setException(new InterruptedException());
      }
    });

    return future;
  }

  public static void close(final HttpResponse response) {
    if (response == null) {
      return;
    }

    final HttpEntity entity = response.getEntity();
    if (entity != null) {
      EntityUtils.consumeQuietly(entity);
    }
  }

  protected static String urlEscape(final String s) {
    try {
      return URLEncoder.encode(s, "UTF-8");
    } catch (final UnsupportedEncodingException e) {
      throw new IllegalStateException();
    }
  }

  static String format(final Object o) {
    try {
      return gson.toJson(o);
    } catch (Exception e) {
      return "Error formatting: " + e.getMessage();
    }
  }
}