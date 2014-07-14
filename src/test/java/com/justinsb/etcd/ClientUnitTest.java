package com.justinsb.etcd;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;

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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class ClientUnitTest {

  private HttpUriRequest httpUriRequest;

  private CloseableHttpAsyncClient makeClient(final HttpResponse httpResponse) {
    CloseableHttpAsyncClient mock = mock(CloseableHttpAsyncClient.class);
    when(mock.execute(any(HttpUriRequest.class), any(FutureCallback.class)))
        .then(new Answer<Future<HttpResponse>>() {
          @Override
          public Future<HttpResponse> answer(InvocationOnMock invocation)
              throws Throwable {

            httpUriRequest = (HttpUriRequest) invocation.getArguments()[0];
            FutureCallback<HttpResponse> callback = (FutureCallback<HttpResponse>) invocation
                .getArguments()[1];
            callback.completed(httpResponse);

            return null;
          }
        });

    return mock;
  }

  private HttpResponse newResponse(final int statusCode, final String reason) {
    HttpResponse response = new BasicHttpResponse(new StatusLine() {

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

    return response;
  }

  private HttpResponse newResponse(final int statusCode, final String reason,
      final EtcdResult result) {
    BasicHttpResponse response = new BasicHttpResponse(new StatusLine() {

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
        } catch (UnsupportedEncodingException e) {
          throw new RuntimeException(e);
        }
      }
    };

    return response;
  }

  @Test
  public void testSuccesfulSetKey() throws Exception {

    httpUriRequest = null;
    CloseableHttpAsyncClient client = makeClient(newResponse(200, "OK"));
    EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"),
        client);
    etcdClient.set("somekey", "somevalue");

    assertNotNull(httpUriRequest);
    assertEquals("PUT", httpUriRequest.getMethod());
    assertEquals(HttpPut.class, httpUriRequest.getClass());
    HttpPut httpPut = (HttpPut) httpUriRequest;

    assertEquals(new URI("http://unknownhost/v2/keys/somekey"),
        httpPut.getURI());
    assertNotNull(httpPut.getEntity());
    assertEquals(UrlEncodedFormEntity.class, httpPut.getEntity().getClass());

    UrlEncodedFormEntity entity = (UrlEncodedFormEntity) httpPut.getEntity();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    entity.writeTo(baos);

    String value = new String(baos.toByteArray());

    assertEquals("value=somevalue", value);

  }

  @Test
  public void testErrorSetKey() throws Exception {
    httpUriRequest = null;
    CloseableHttpAsyncClient client = makeClient(newResponse(500,
        "Internal Server Error"));
    EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"),
        client);

    try {
      etcdClient.set("somekey", "somevalue");
      fail();
    } catch (EtcdClientException e) {
      assertNotNull(httpUriRequest);
      assertEquals("PUT", httpUriRequest.getMethod());
      assertEquals(HttpPut.class, httpUriRequest.getClass());
      HttpPut httpPut = (HttpPut) httpUriRequest;

      assertEquals(new URI("http://unknownhost/v2/keys/somekey"),
          httpPut.getURI());
      assertNotNull(httpPut.getEntity());
      assertEquals(UrlEncodedFormEntity.class, httpPut.getEntity().getClass());

      UrlEncodedFormEntity entity = (UrlEncodedFormEntity) httpPut.getEntity();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      entity.writeTo(baos);

      String value = new String(baos.toByteArray());

      assertEquals("value=somevalue", value);

      assertTrue(e.getMessage().contains("Internal Server Error"));
    }
  }

  @Test
  public void testCas() throws Exception {

    httpUriRequest = null;
    CloseableHttpAsyncClient client = makeClient(newResponse(200, "OK"));
    EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"),
        client);

    etcdClient.cas("somekey", "someValue", "someValue2");

    assertNotNull(httpUriRequest);
    assertEquals("PUT", httpUriRequest.getMethod());
    assertEquals(HttpPut.class, httpUriRequest.getClass());
    HttpPut httpPut = (HttpPut) httpUriRequest;

    assertEquals(new URI("http://unknownhost/v2/keys/somekey"),
        httpPut.getURI());
    assertNotNull(httpPut.getEntity());
    assertEquals(UrlEncodedFormEntity.class, httpPut.getEntity().getClass());

    UrlEncodedFormEntity entity = (UrlEncodedFormEntity) httpPut.getEntity();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    entity.writeTo(baos);

    String value = new String(baos.toByteArray());

    assertEquals("value=someValue2&prevValue=someValue", value);

  }

  @Test
  public void testCasWrongPreviousValue() throws Exception {
    EtcdResult expectedResult = new EtcdResult();
    expectedResult.errorCode = 101;

    httpUriRequest = null;
    CloseableHttpAsyncClient client = makeClient(newResponse(412,
        "Precondition failed", expectedResult));
    EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"),
        client);

    EtcdResult result = etcdClient.cas("somekey", "someValue", "someValue2");

    assertTrue(result.isError());

  }

  @Test
  public void testCasErrorCodeNotExpected() throws Exception {
    EtcdResult expectedResult = new EtcdResult();
    expectedResult.errorCode = 1;

    httpUriRequest = null;
    CloseableHttpAsyncClient client = makeClient(newResponse(412,
        "Precondition failed", expectedResult));
    EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"),
        client);

    try {
      etcdClient.cas("somekey", "someValue", "someValue2");
      fail();
    } catch (EtcdClientException e) {

    }

  }

  @Test
  public void testGetKey() throws Exception {
    httpUriRequest = null;
    EtcdResult etcdResult = new EtcdResult();
    etcdResult.action = "get";

    EtcdNode node = new EtcdNode();
    node.key = "/somekey";
    node.value = "somevalue";

    etcdResult.node = node;

    CloseableHttpAsyncClient client = makeClient(newResponse(200, "OK",
        etcdResult));
    EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"),
        client);

    EtcdResult result = etcdClient.get("somekey");

    assertNotNull(httpUriRequest);
    assertEquals("GET", httpUriRequest.getMethod());
    assertEquals(HttpGet.class, httpUriRequest.getClass());
    HttpGet httpGet = (HttpGet) httpUriRequest;

    assertEquals(new URI("http://unknownhost/v2/keys/somekey"),
        httpGet.getURI());

    assertNotNull(result);
    assertNotNull(result.node);
    assertEquals("somevalue", result.node.value);

  }

  @Test
  public void testGetKeyNotFound() throws Exception {
    httpUriRequest = null;
    EtcdResult etcdResult = new EtcdResult();
    etcdResult.errorCode = 100;
    etcdResult.message = "Key not found";

    CloseableHttpAsyncClient client = makeClient(newResponse(404, "Not Found",
        etcdResult));
    EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"),
        client);

    EtcdResult result = etcdClient.get("somekey");

    assertNotNull(httpUriRequest);
    assertEquals("GET", httpUriRequest.getMethod());
    assertEquals(HttpGet.class, httpUriRequest.getClass());
    HttpGet httpGet = (HttpGet) httpUriRequest;

    assertEquals(new URI("http://unknownhost/v2/keys/somekey"),
        httpGet.getURI());

    assertNull(result);

  }

  @Test
  public void testDeleteKey() throws Exception {
    httpUriRequest = null;
    EtcdResult etcdResult = new EtcdResult();
    etcdResult.action = "delete";

    EtcdNode node = new EtcdNode();
    node.key = "/somekey";

    EtcdNode previousNode = new EtcdNode();
    previousNode.key = "/somekey";
    previousNode.value = "somevalue";

    etcdResult.node = node;
    etcdResult.prevNode = previousNode;

    CloseableHttpAsyncClient client = makeClient(newResponse(200, "OK",
        etcdResult));
    EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"),
        client);

    EtcdResult result = etcdClient.delete("somekey");

    assertNotNull(httpUriRequest);
    assertEquals("DELETE", httpUriRequest.getMethod());
    assertEquals(HttpDelete.class, httpUriRequest.getClass());
    HttpDelete httpDelete = (HttpDelete) httpUriRequest;

    assertEquals(new URI("http://unknownhost/v2/keys/somekey"),
        httpDelete.getURI());

    assertNotNull(result);
    assertNotNull(result.node);
    assertNull(result.node.value);

    assertNotNull(result.prevNode);
    assertNotNull(result.prevNode.key);
  }

  @Test
  public void testDeleteKeyNotFound() throws Exception {
    httpUriRequest = null;
    EtcdResult etcdResult = new EtcdResult();
    etcdResult.errorCode = 100;

    CloseableHttpAsyncClient client = makeClient(newResponse(404, "Not Found",
        etcdResult));
    EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"),
        client);
    try {
      etcdClient.delete("somekey");
      fail();
    }

    catch (EtcdClientException e) {

      assertNotNull(httpUriRequest);
      assertEquals("DELETE", httpUriRequest.getMethod());
      assertEquals(HttpDelete.class, httpUriRequest.getClass());
      HttpDelete httpDelete = (HttpDelete) httpUriRequest;

      assertEquals(new URI("http://unknownhost/v2/keys/somekey"),
          httpDelete.getURI());
    }
  }

  @Test
  public void testDeleteDiretory() throws Exception {
    httpUriRequest = null;
    EtcdResult etcdResult = new EtcdResult();
    etcdResult.action = "delete";

    CloseableHttpAsyncClient client = makeClient(newResponse(202, "Accepted",
        etcdResult));
    EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"),
        client);

    EtcdResult result = etcdClient.deleteDirectory("somekey");

    assertNotNull(httpUriRequest);
    assertEquals("DELETE", httpUriRequest.getMethod());
    assertEquals(HttpDelete.class, httpUriRequest.getClass());
    HttpDelete httpDelete = (HttpDelete) httpUriRequest;

    assertEquals(new URI("http://unknownhost/v2/keys/somekey?dir=true"),
        httpDelete.getURI());

    assertNotNull(result);

  }

  @Test
  public void testDeleteKeyButDirectory() throws Exception {
    httpUriRequest = null;
    EtcdResult etcdResult = new EtcdResult();
    etcdResult.errorCode = 102;
    etcdResult.message = "Not a file";

    CloseableHttpAsyncClient client = makeClient(newResponse(403, "Forbidden",
        etcdResult));
    EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"),
        client);
    try {
      etcdClient.delete("somekey");
      fail();
    }

    catch (EtcdClientException e) {

      assertNotNull(httpUriRequest);
      assertEquals("DELETE", httpUriRequest.getMethod());
      assertEquals(HttpDelete.class, httpUriRequest.getClass());
      HttpDelete httpDelete = (HttpDelete) httpUriRequest;

      assertEquals(new URI("http://unknownhost/v2/keys/somekey"),
          httpDelete.getURI());
    }
  }

  @Test
  public void testDeleteDirectoryNotEmpty() throws Exception {
    httpUriRequest = null;
    EtcdResult etcdResult = new EtcdResult();
    etcdResult.errorCode = 108;
    etcdResult.message = "Directory not empty";

    CloseableHttpAsyncClient client = makeClient(newResponse(403, "Forbidden",
        etcdResult));
    EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"),
        client);
    try {
      etcdClient.deleteDirectory("somekey");
      fail();
    }

    catch (EtcdClientException e) {

      assertNotNull(httpUriRequest);
      assertEquals("DELETE", httpUriRequest.getMethod());
      assertEquals(HttpDelete.class, httpUriRequest.getClass());
      HttpDelete httpDelete = (HttpDelete) httpUriRequest;

      assertEquals(new URI("http://unknownhost/v2/keys/somekey?dir=true"),
          httpDelete.getURI());
    }

  }
  
  @Test
  public void testListingChildren() throws Exception {
    httpUriRequest = null;
    EtcdResult etcdResult = new EtcdResult();
    etcdResult.action = "delete";

    EtcdNode node = new EtcdNode();
    node.key = "/dir";
    
    EtcdNode nodeOne = new EtcdNode();
    nodeOne.key ="/dir/key1";
    nodeOne.value = "somevalue";
    
    EtcdNode nodeTwo = new EtcdNode();
    nodeTwo.key = "/dir/key2";
    nodeTwo.value = "somevalue";
    
    node.nodes = Arrays.asList(nodeOne, nodeTwo);

    etcdResult.node = node;

    CloseableHttpAsyncClient client = makeClient(newResponse(200, "OK",
        etcdResult));
    EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"),
        client);

    EtcdResult result = etcdClient.listChildren("dir");

    assertNotNull(httpUriRequest);
    assertEquals("GET", httpUriRequest.getMethod());
    assertEquals(HttpGet.class, httpUriRequest.getClass());
    HttpGet httpGet = (HttpGet) httpUriRequest;

    assertEquals(new URI("http://unknownhost/v2/keys/dir/"),
        httpGet.getURI());

    assertNotNull(result);
    assertNotNull(result.node);
    assertNotNull(result.node.nodes);

    assertEquals(2, result.node.nodes.size());
  }
  
  @Test
  public void testListingDirectory() throws Exception {
    httpUriRequest = null;
    EtcdResult etcdResult = new EtcdResult();
    etcdResult.action = "delete";

    EtcdNode node = new EtcdNode();
    node.key = "/dir";
    
    EtcdNode nodeOne = new EtcdNode();
    nodeOne.key ="/dir/key1";
    nodeOne.value = "somevalue";
    
    EtcdNode nodeTwo = new EtcdNode();
    nodeTwo.key = "/dir/key2";
    nodeTwo.value = "somevalue";
    
    node.nodes = Arrays.asList(nodeOne, nodeTwo);

    etcdResult.node = node;

    CloseableHttpAsyncClient client = makeClient(newResponse(200, "OK",
        etcdResult));
    EtcdClient etcdClient = new EtcdClient(new URI("http://unknownhost"),
        client);

    List<EtcdNode> nodes = etcdClient.listDirectory("dir");
    

    assertNotNull(httpUriRequest);
    assertEquals("GET", httpUriRequest.getMethod());
    assertEquals(HttpGet.class, httpUriRequest.getClass());
    HttpGet httpGet = (HttpGet) httpUriRequest;

    assertEquals(new URI("http://unknownhost/v2/keys/dir/"),
        httpGet.getURI());

    assertNotNull(nodes);
    
    assertEquals(2, nodes.size());
    assertEquals("/dir/key1", nodes.get(0).key);
    assertEquals("somevalue", nodes.get(0).value);
    
    assertEquals("/dir/key2", nodes.get(1).key);
    assertEquals("somevalue", nodes.get(1).value);
    
  }

}
