package com.justinsb.etcd;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.message.BasicHttpResponse;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("Duplicates")
public class JetcdUnitTest {
  private HttpUriRequest httpUriRequest;

  @SuppressWarnings("unchecked")
  private CloseableHttpAsyncClient makeClient(final HttpResponse httpResponse) {
    final CloseableHttpAsyncClient mock = mock(CloseableHttpAsyncClient.class);
    when(mock.execute(any(HttpUriRequest.class), any(FutureCallback.class))).then((Answer<Future<HttpResponse>>) invocation -> {
      httpUriRequest = (HttpUriRequest) invocation.getArguments()[0];
      final FutureCallback<HttpResponse> callback = (FutureCallback<HttpResponse>) invocation.getArguments()[1];
      callback.completed(httpResponse);

      return null;
    });

    return mock;
  }

  private HttpResponse newResponse(final int statusCode, final String reason) {
    return new BasicHttpResponse(new StatusLine() {
      @Override
      public int getStatusCode() {
        return statusCode;
      }

      @Override
      public String getReasonPhrase() {
        // TODO Auto-generated method stub
        return reason;
      }

      @Override
      public ProtocolVersion getProtocolVersion() {
        return new ProtocolVersion("HTTP", 1, 1);
      }
    });
  }

  private HttpResponse newResponse(final int statusCode, final String reason, final EtcdResult result) {
    return new BasicHttpResponse(new StatusLine() {
      @Override
      public int getStatusCode() {
        return statusCode;
      }

      @Override
      public String getReasonPhrase() {
        // TODO Auto-generated method stub
        return reason;
      }

      @Override
      public ProtocolVersion getProtocolVersion() {
        return new ProtocolVersion("HTTP", 1, 1);
      }

    }) {
      @Override
      public HttpEntity getEntity() {
        try {
          return new StringEntity(EtcdClient.format(result));
        } catch (final UnsupportedEncodingException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  @Test
  public void testSuccesfulSetKey() throws Exception {
    httpUriRequest = null;
    final CloseableHttpAsyncClient client = makeClient(newResponse(200, "OK"));
    final EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"), client);
    etcdClient.set("somekey", "somevalue");

    assertNotNull(httpUriRequest);
    assertEquals("PUT", httpUriRequest.getMethod());
    assertEquals(HttpPut.class, httpUriRequest.getClass());
    final HttpPut httpPut = (HttpPut) httpUriRequest;

    assertEquals(new URI("http://unknownhost/v2/keys/somekey"), httpPut.getURI());
    assertNotNull(httpPut.getEntity());
    assertEquals(UrlEncodedFormEntity.class, httpPut.getEntity().getClass());

    final UrlEncodedFormEntity entity = (UrlEncodedFormEntity) httpPut.getEntity();
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    entity.writeTo(baos);

    final String value = new String(baos.toByteArray());

    assertEquals("value=somevalue", value);
  }

  @Test
  public void testErrorSetKey() throws Exception {
    httpUriRequest = null;
    final CloseableHttpAsyncClient client = makeClient(newResponse(500, "Internal Server Error"));
    final EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"), client);

    try {
      etcdClient.set("somekey", "somevalue");
      fail();
    } catch (final EtcdClientException e) {
      assertNotNull(httpUriRequest);
      assertEquals("PUT", httpUriRequest.getMethod());
      assertEquals(HttpPut.class, httpUriRequest.getClass());
      final HttpPut httpPut = (HttpPut) httpUriRequest;

      assertEquals(new URI("http://unknownhost/v2/keys/somekey"), httpPut.getURI());
      assertNotNull(httpPut.getEntity());
      assertEquals(UrlEncodedFormEntity.class, httpPut.getEntity().getClass());

      final UrlEncodedFormEntity entity = (UrlEncodedFormEntity) httpPut.getEntity();
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      entity.writeTo(baos);

      final String value = new String(baos.toByteArray());

      assertEquals("value=somevalue", value);

      assertTrue(e.getMessage().contains("Internal Server Error"));
    }
  }

  @Test
  public void testCas() throws Exception {
    httpUriRequest = null;
    final CloseableHttpAsyncClient client = makeClient(newResponse(200, "OK"));
    final EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"), client);

    etcdClient.cas("somekey", "someValue", "someValue2");

    assertNotNull(httpUriRequest);
    assertEquals("PUT", httpUriRequest.getMethod());
    assertEquals(HttpPut.class, httpUriRequest.getClass());
    final HttpPut httpPut = (HttpPut) httpUriRequest;

    assertEquals(new URI("http://unknownhost/v2/keys/somekey"), httpPut.getURI());
    assertNotNull(httpPut.getEntity());
    assertEquals(UrlEncodedFormEntity.class, httpPut.getEntity().getClass());

    final UrlEncodedFormEntity entity = (UrlEncodedFormEntity) httpPut.getEntity();
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    entity.writeTo(baos);

    final String value = new String(baos.toByteArray());

    assertEquals("value=someValue2&prevValue=someValue", value);
  }

  @Test
  public void testCasWrongPreviousValue() throws Exception {
    final EtcdResult expectedResult = new EtcdResult();
    expectedResult.errorCode = 101;

    httpUriRequest = null;
    final CloseableHttpAsyncClient client = makeClient(newResponse(412, "Precondition failed", expectedResult));
    final EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"), client);

    final EtcdResult result = etcdClient.cas("somekey", "someValue", "someValue2");

    assertTrue(result.isError());
  }

  @Test
  public void testCasErrorCodeNotExpected() throws Exception {
    final EtcdResult expectedResult = new EtcdResult();
    expectedResult.errorCode = 1;

    httpUriRequest = null;
    final CloseableHttpAsyncClient client = makeClient(newResponse(412, "Precondition failed", expectedResult));
    final EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"), client);

    try {
      etcdClient.cas("somekey", "someValue", "someValue2");
      fail();
    } catch (final EtcdClientException ece) {
      // expected this?
    }
  }

  @Test
  public void testGetKey() throws Exception {
    httpUriRequest = null;
    final EtcdResult etcdResult = new EtcdResult();
    etcdResult.action = "get";

    final EtcdNode node = new EtcdNode();
    node.key = "/somekey";
    node.value = "somevalue";

    etcdResult.node = node;

    final CloseableHttpAsyncClient client = makeClient(newResponse(200, "OK", etcdResult));
    final EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"), client);

    final EtcdResult result = etcdClient.get("somekey");

    assertNotNull(httpUriRequest);
    assertEquals("GET", httpUriRequest.getMethod());
    assertEquals(HttpGet.class, httpUriRequest.getClass());
    final HttpGet httpGet = (HttpGet) httpUriRequest;

    assertEquals(new URI("http://unknownhost/v2/keys/somekey"), httpGet.getURI());

    assertNotNull(result);
    assertNotNull(result.node);
    assertEquals("somevalue", result.node.value);
  }

  @Test
  public void testGetKeyNotFound() throws Exception {
    httpUriRequest = null;
    final EtcdResult etcdResult = new EtcdResult();
    etcdResult.errorCode = 100;
    etcdResult.message = "Key not found";

    final CloseableHttpAsyncClient client = makeClient(newResponse(404, "Not Found", etcdResult));
    final EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"), client);

    final EtcdResult result = etcdClient.get("somekey");

    assertNotNull(httpUriRequest);
    assertEquals("GET", httpUriRequest.getMethod());
    assertEquals(HttpGet.class, httpUriRequest.getClass());
    final HttpGet httpGet = (HttpGet) httpUriRequest;

    assertEquals(new URI("http://unknownhost/v2/keys/somekey"), httpGet.getURI());

    assertNull(result);
  }

  @Test
  public void testDeleteKey() throws Exception {
    httpUriRequest = null;
    final EtcdResult etcdResult = new EtcdResult();
    etcdResult.action = "delete";

    final EtcdNode node = new EtcdNode();
    node.key = "/somekey";

    final EtcdNode previousNode = new EtcdNode();
    previousNode.key = "/somekey";
    previousNode.value = "somevalue";

    etcdResult.node = node;
    etcdResult.prevNode = previousNode;

    final CloseableHttpAsyncClient client = makeClient(newResponse(200, "OK", etcdResult));
    final EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"), client);

    final EtcdResult result = etcdClient.delete("somekey");

    assertNotNull(httpUriRequest);
    assertEquals("DELETE", httpUriRequest.getMethod());
    assertEquals(HttpDelete.class, httpUriRequest.getClass());
    final HttpDelete httpDelete = (HttpDelete) httpUriRequest;

    assertEquals(new URI("http://unknownhost/v2/keys/somekey"), httpDelete.getURI());

    assertNotNull(result);
    assertNotNull(result.node);
    assertNull(result.node.value);

    assertNotNull(result.prevNode);
    assertNotNull(result.prevNode.key);
  }

  @Test
  public void testDeleteKeyNotFound() throws Exception {
    httpUriRequest = null;
    final EtcdResult etcdResult = new EtcdResult();
    etcdResult.errorCode = 100;

    final CloseableHttpAsyncClient client = makeClient(newResponse(404, "Not Found",
      etcdResult));
    final EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"),
      client);
    try {
      etcdClient.delete("somekey");
      fail();
    } catch (final EtcdClientException ece) {
      assertNotNull(httpUriRequest);
      assertEquals("DELETE", httpUriRequest.getMethod());
      assertEquals(HttpDelete.class, httpUriRequest.getClass());
      final HttpDelete httpDelete = (HttpDelete) httpUriRequest;

      assertEquals(new URI("http://unknownhost/v2/keys/somekey"), httpDelete.getURI());
    }
  }

  @Test
  public void testDeleteDiretory() throws Exception {
    httpUriRequest = null;
    final EtcdResult etcdResult = new EtcdResult();
    etcdResult.action = "delete";

    final CloseableHttpAsyncClient client = makeClient(newResponse(202, "Accepted", etcdResult));
    final EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"), client);

    final EtcdResult result = etcdClient.deleteDirectory("somekey");

    assertNotNull(httpUriRequest);
    assertEquals("DELETE", httpUriRequest.getMethod());
    assertEquals(HttpDelete.class, httpUriRequest.getClass());
    final HttpDelete httpDelete = (HttpDelete) httpUriRequest;

    assertEquals(new URI("http://unknownhost/v2/keys/somekey?dir=true"), httpDelete.getURI());

    assertNotNull(result);
  }

  @Test
  public void testDeleteKeyButDirectory() throws Exception {
    httpUriRequest = null;
    final EtcdResult etcdResult = new EtcdResult();
    etcdResult.errorCode = 102;
    etcdResult.message = "Not a file";

    final CloseableHttpAsyncClient client = makeClient(newResponse(403, "Forbidden", etcdResult));
    final EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"), client);
    try {
      etcdClient.delete("somekey");
      fail();
    } catch (final EtcdClientException e) {
      assertNotNull(httpUriRequest);
      assertEquals("DELETE", httpUriRequest.getMethod());
      assertEquals(HttpDelete.class, httpUriRequest.getClass());
      final HttpDelete httpDelete = (HttpDelete) httpUriRequest;

      assertEquals(new URI("http://unknownhost/v2/keys/somekey"), httpDelete.getURI());
    }
  }

  @Test
  public void testDeleteDirectoryNotEmpty() throws Exception {
    httpUriRequest = null;
    final EtcdResult etcdResult = new EtcdResult();
    etcdResult.errorCode = 108;
    etcdResult.message = "Directory not empty";

    final CloseableHttpAsyncClient client = makeClient(newResponse(403, "Forbidden", etcdResult));
    final EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"), client);
    try {
      etcdClient.deleteDirectory("somekey");
      fail();
    } catch (final EtcdClientException ece) {
      assertNotNull(httpUriRequest);
      assertEquals("DELETE", httpUriRequest.getMethod());
      assertEquals(HttpDelete.class, httpUriRequest.getClass());
      final HttpDelete httpDelete = (HttpDelete) httpUriRequest;

      assertEquals(new URI("http://unknownhost/v2/keys/somekey?dir=true"), httpDelete.getURI());
    }
  }

  @Test
  public void testListingChildren() throws Exception {
    httpUriRequest = null;
    final EtcdResult etcdResult = new EtcdResult();
    etcdResult.action = "delete";

    final EtcdNode node = new EtcdNode();
    node.key = "/dir";

    final EtcdNode nodeOne = new EtcdNode();
    nodeOne.key = "/dir/key1";
    nodeOne.value = "somevalue";

    final EtcdNode nodeTwo = new EtcdNode();
    nodeTwo.key = "/dir/key2";
    nodeTwo.value = "somevalue";

    node.nodes = Arrays.asList(nodeOne, nodeTwo);

    etcdResult.node = node;

    final CloseableHttpAsyncClient client = makeClient(newResponse(200, "OK", etcdResult));
    final EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"), client);

    final EtcdResult result = etcdClient.listChildren("dir");

    assertNotNull(httpUriRequest);
    assertEquals("GET", httpUriRequest.getMethod());
    assertEquals(HttpGet.class, httpUriRequest.getClass());
    final HttpGet httpGet = (HttpGet) httpUriRequest;

    assertEquals(new URI("http://unknownhost/v2/keys/dir/"), httpGet.getURI());

    assertNotNull(result);
    assertNotNull(result.node);
    assertNotNull(result.node.nodes);

    assertEquals(2, result.node.nodes.size());
  }

  @Test
  public void testListingDirectory() throws Exception {
    httpUriRequest = null;
    final EtcdResult etcdResult = new EtcdResult();
    etcdResult.action = "delete";

    final EtcdNode node = new EtcdNode();
    node.key = "/dir";

    final EtcdNode nodeOne = new EtcdNode();
    nodeOne.key = "/dir/key1";
    nodeOne.value = "somevalue";

    final EtcdNode nodeTwo = new EtcdNode();
    nodeTwo.key = "/dir/key2";
    nodeTwo.value = "somevalue";

    node.nodes = Arrays.asList(nodeOne, nodeTwo);

    etcdResult.node = node;

    final CloseableHttpAsyncClient client = makeClient(newResponse(200, "OK", etcdResult));
    final EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"), client);

    final List<EtcdNode> nodes = etcdClient.listDirectory("dir");

    assertNotNull(httpUriRequest);
    assertEquals("GET", httpUriRequest.getMethod());
    assertEquals(HttpGet.class, httpUriRequest.getClass());
    final HttpGet httpGet = (HttpGet) httpUriRequest;

    assertEquals(new URI("http://unknownhost/v2/keys/dir/"), httpGet.getURI());

    assertNotNull(nodes);

    assertEquals(2, nodes.size());
    assertEquals("/dir/key1", nodes.get(0).key);
    assertEquals("somevalue", nodes.get(0).value);

    assertEquals("/dir/key2", nodes.get(1).key);
    assertEquals("somevalue", nodes.get(1).value);
  }
}
