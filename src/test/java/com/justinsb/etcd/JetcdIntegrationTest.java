package com.justinsb.etcd;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.util.concurrent.ListenableFuture;

@SuppressWarnings("Duplicates")
public class JetcdIntegrationTest {
  private String prefix;
  private EtcdClient client;

  @Before
  public void initialize() {
    final String host = System.getProperty("etcdHost");

    this.prefix = "/unittest-" + UUID.randomUUID().toString();
    this.client = new EtcdClient(URI.create(host == null || host.isEmpty() ? "http://127.0.0.1:4001/" : host));
  }

  @Test
  public void testEtcdOffline() throws URISyntaxException {
    final EtcdClient client = new EtcdClient(new URI("http://unknowhost"));
    try {
      client.get("key");
      Assert.fail();
    } catch (final EtcdClientException ece) {
      // what now?
    }
  }

  @Test
  public void setAndGet() throws Exception {
    final String key = prefix + "/message";

    EtcdResult result;

    result = this.client.set(key, "hello");
    Assert.assertEquals("set", result.action);
    Assert.assertEquals("hello", result.node.value);
    Assert.assertNull(result.prevNode);

    result = this.client.get(key);
    Assert.assertEquals("get", result.action);
    Assert.assertEquals("hello", result.node.value);
    Assert.assertNull(result.prevNode);

    result = this.client.set(key, "world");
    Assert.assertEquals("set", result.action);
    Assert.assertEquals("world", result.node.value);
    Assert.assertNotNull(result.prevNode);
    Assert.assertEquals("hello", result.prevNode.value);

    result = this.client.get(key);
    Assert.assertEquals("get", result.action);
    Assert.assertEquals("world", result.node.value);
    Assert.assertNull(result.prevNode);
  }

  @Test
  public void getNonExistentKey() throws Exception {
    final String key = prefix + "/doesnotexist";

    final EtcdResult result;

    result = this.client.get(key);
    Assert.assertNull(result);
  }

  @Test
  public void testDelete() throws Exception {
    final String key = prefix + "/testDelete";

    EtcdResult result;

    this.client.set(key, "hello");

    result = this.client.get(key);
    Assert.assertEquals("hello", result.node.value);

    result = this.client.delete(key);
    Assert.assertEquals("delete", result.action);
    Assert.assertEquals(null, result.node.value);
    Assert.assertEquals("hello", result.prevNode.value);

    result = this.client.get(key);
    Assert.assertNull(result);
  }

  @Test
  public void deleteNonExistentKey() throws Exception {
    final String key = prefix + "/doesnotexist";

    try {
      this.client.delete(key);
      Assert.fail();
    } catch (final EtcdClientException e) {
      Assert.assertTrue(e.isEtcdError(100));
    }
  }

  @Test
  public void testTtl() throws Exception {
    final String key = prefix + "/ttl";

    EtcdResult result;

    result = this.client.set(key, "hello", 2);
    Assert.assertNotNull(result.node.expiration);
    Assert.assertTrue(result.node.ttl == 2 || result.node.ttl == 1);

    result = this.client.get(key);
    Assert.assertEquals("hello", result.node.value);

    // TTL was redefined to mean TTL + 0.5s (Issue #306)
    Thread.sleep(3000);

    result = this.client.get(key);
    Assert.assertNull(result);
  }

  @Test
  public void testCAS() throws Exception {
    final String key = prefix + "/cas";

    EtcdResult result;

    this.client.set(key, "hello");
    result = this.client.get(key);
    Assert.assertEquals("hello", result.node.value);

    result = this.client.cas(key, "world", "world");
    Assert.assertEquals(true, result.isError());
    result = this.client.get(key);
    Assert.assertEquals("hello", result.node.value);

    result = this.client.cas(key, "hello", "world");
    Assert.assertEquals(false, result.isError());
    result = this.client.get(key);
    Assert.assertEquals("world", result.node.value);
  }

  @Test
  public void testWatchPrefix() throws Exception {
    final String key = prefix + "/watch";

    EtcdResult result = this.client.set(key + "/f2", "f2");
    Assert.assertTrue(!result.isError());
    Assert.assertNotNull(result.node);
    Assert.assertEquals("f2", result.node.value);

    final ListenableFuture<EtcdResult> watchFuture = this.client.watch(key,
        result.node.modifiedIndex + 1, true);
    try {
      final EtcdResult watchResult = watchFuture.get(100, TimeUnit.MILLISECONDS);
      Assert.fail("Subtree watch fired unexpectedly: " + watchResult);
    } catch (final TimeoutException e) {
      // Expected
    }

    Assert.assertFalse(watchFuture.isDone());

    result = this.client.set(key + "/f1", "f1");
    Assert.assertTrue(!result.isError());
    Assert.assertNotNull(result.node);
    Assert.assertEquals("f1", result.node.value);

    final EtcdResult watchResult = watchFuture.get(100, TimeUnit.MILLISECONDS);

    Assert.assertNotNull(watchResult);
    Assert.assertTrue(!watchResult.isError());
    Assert.assertNotNull(watchResult.node);

    {
      Assert.assertEquals(key + "/f1", watchResult.node.key);
      Assert.assertEquals("f1", watchResult.node.value);
      Assert.assertEquals("set", watchResult.action);
      Assert.assertNull(result.prevNode);
      Assert.assertEquals(result.node.modifiedIndex,
          watchResult.node.modifiedIndex);
    }
  }

  @Test
  public void testList() throws Exception {
    final String key = prefix + "/dir";

    EtcdResult result;

    result = this.client.set(key + "/f1", "f1");
    Assert.assertEquals("f1", result.node.value);
    result = this.client.set(key + "/f2", "f2");
    Assert.assertEquals("f2", result.node.value);
    result = this.client.set(key + "/f3", "f3");
    Assert.assertEquals("f3", result.node.value);
    result = this.client.set(key + "/subdir1/f", "f");
    Assert.assertEquals("f", result.node.value);

    final EtcdResult listing = this.client.listChildren(key);
    Assert.assertEquals(4, listing.node.nodes.size());
    Assert.assertEquals("get", listing.action);

    {
      final EtcdNode child = listing.node.nodes.get(0);
      Assert.assertEquals(key + "/f1", child.key);
      Assert.assertEquals("f1", child.value);
      Assert.assertEquals(false, child.dir);
    }
    {
      final EtcdNode child = listing.node.nodes.get(1);
      Assert.assertEquals(key + "/f2", child.key);
      Assert.assertEquals("f2", child.value);
      Assert.assertEquals(false, child.dir);
    }
    {
      final EtcdNode child = listing.node.nodes.get(2);
      Assert.assertEquals(key + "/f3", child.key);
      Assert.assertEquals("f3", child.value);
      Assert.assertEquals(false, child.dir);
    }
    {
      final EtcdNode child = listing.node.nodes.get(3);
      Assert.assertEquals(key + "/subdir1", child.key);
      Assert.assertEquals(null, child.value);
      Assert.assertEquals(true, child.dir);
    }
  }

  @Test
  public void testGetVersion() throws Exception {
    final String version = this.client.getVersion();
    Assert.assertTrue("Expected version 'etcd 2.0...', but got " + version, version.startsWith("{\"releaseVersion\":\"2.0"));
  }
}
