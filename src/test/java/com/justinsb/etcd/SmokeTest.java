package com.justinsb.etcd;

import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.util.concurrent.ListenableFuture;
import com.justinsb.etcd.EtcdClient;
import com.justinsb.etcd.EtcdClientException;
import com.justinsb.etcd.EtcdResult;

public class SmokeTest {
    String prefix;
    EtcdClient client;

    @Before
    public void initialize() {
        this.prefix = "/unittest-" + UUID.randomUUID().toString();
        this.client = new EtcdClient(URI.create("http://127.0.0.1:4001/"));
    }

    @Test
    public void setAndGet() throws Exception {
        String key = prefix + "/message";

        EtcdResult result;

        result = this.client.set(key, "hello");
        Assert.assertEquals("SET", result.action);
        Assert.assertEquals("hello", result.value);
        Assert.assertEquals(null, result.prevValue);
        Assert.assertEquals(true, result.newKey);

        result = this.client.get(key);
        Assert.assertEquals("GET", result.action);
        Assert.assertEquals("hello", result.value);
        Assert.assertEquals(null, result.prevValue);
        Assert.assertEquals(false, result.newKey);

        result = this.client.set(key, "world");
        Assert.assertEquals("SET", result.action);
        Assert.assertEquals("world", result.value);
        Assert.assertEquals("hello", result.prevValue);
        Assert.assertEquals(false, result.newKey);

        result = this.client.get(key);
        Assert.assertEquals("GET", result.action);
        Assert.assertEquals("world", result.value);
        Assert.assertEquals(null, result.prevValue);
        Assert.assertEquals(false, result.newKey);
    }

    @Test
    public void getNonExistentKey() throws Exception {
        String key = prefix + "/doesnotexist";

        EtcdResult result;

        result = this.client.get(key);
        Assert.assertNull(result);
    }

    @Test
    public void testDelete() throws Exception {
        String key = prefix + "/testDelete";

        EtcdResult result;

        result = this.client.set(key, "hello");

        result = this.client.get(key);
        Assert.assertEquals("hello", result.value);

        result = this.client.delete(key);
        Assert.assertEquals("DELETE", result.action);
        Assert.assertEquals(null, result.value);
        Assert.assertEquals("hello", result.prevValue);
        Assert.assertEquals(false, result.newKey);

        result = this.client.get(key);
        Assert.assertNull(result);
    }

    @Test
    public void deleteNonExistentKey() throws Exception {
        String key = prefix + "/doesnotexist";

        try {
            EtcdResult result = this.client.delete(key);
            Assert.fail();
        } catch (EtcdClientException e) {
            Assert.assertTrue(e.isEtcdError(100));
        }
    }

    @Test
    public void testTtl() throws Exception {
        String key = prefix + "/ttl";

        EtcdResult result;

        result = this.client.set(key, "hello", 2);
        Assert.assertNotNull(result.expiration);
        Assert.assertTrue(result.ttl == 2 || result.ttl == 1);

        result = this.client.get(key);
        Assert.assertEquals("hello", result.value);

        Thread.sleep(2000);

        result = this.client.get(key);
        Assert.assertNull(result);
    }

    @Test
    public void testCAS() throws Exception {
        String key = prefix + "/cas";

        EtcdResult result;

        result = this.client.set(key, "hello");
        result = this.client.get(key);
        Assert.assertEquals("hello", result.value);

        result = this.client.cas(key, "world", "world");
        Assert.assertEquals(true, result.isError());
        result = this.client.get(key);
        Assert.assertEquals("hello", result.value);

        result = this.client.cas(key, "hello", "world");
        Assert.assertEquals(false, result.isError());
        result = this.client.get(key);
        Assert.assertEquals("world", result.value);
    }

    @Test
    public void testWatchPrefix() throws Exception {
        String key = prefix + "/watch";

        EtcdResult result = this.client.set(key + "/f2", "f2");
        Assert.assertEquals("f2", result.value);

        ListenableFuture<EtcdResult> watchFuture = this.client.watch(key, result.index + 1);
        try {
            EtcdResult watchResult = watchFuture.get(100, TimeUnit.MILLISECONDS);
            Assert.fail("Subtree watch fired unexpectedly: " + watchResult);
        } catch (TimeoutException e) {
            // Expected
        }

        Assert.assertFalse(watchFuture.isDone());

        result = this.client.set(key + "/f1", "f1");
        Assert.assertEquals("f1", result.value);

        EtcdResult watchResult = watchFuture.get(100, TimeUnit.MILLISECONDS);

        Assert.assertNotNull(result);

        {
            Assert.assertEquals(key + "/f1", watchResult.key);
            Assert.assertEquals("f1", watchResult.value);
            Assert.assertEquals("SET", watchResult.action);
            Assert.assertEquals(true, watchResult.newKey);
            Assert.assertEquals(result.index, watchResult.index);
        }
    }

    @Test
    public void testList() throws Exception {
        String key = prefix + "/dir";

        EtcdResult result;

        result = this.client.set(key + "/f1", "f1");
        Assert.assertEquals("f1", result.value);
        result = this.client.set(key + "/f2", "f2");
        Assert.assertEquals("f2", result.value);
        result = this.client.set(key + "/f3", "f3");
        Assert.assertEquals("f3", result.value);
        result = this.client.set(key + "/subdir1/f", "f");
        Assert.assertEquals("f", result.value);

        List<EtcdResult> listing = this.client.listChildren(key);
        Assert.assertEquals(4, listing.size());

        {
            result = listing.get(0);
            Assert.assertEquals(key + "/f1", result.key);
            Assert.assertEquals("f1", result.value);
            Assert.assertEquals("GET", result.action);
            Assert.assertEquals(false, result.dir);
        }
        {
            result = listing.get(1);
            Assert.assertEquals(key + "/f2", result.key);
            Assert.assertEquals("f2", result.value);
            Assert.assertEquals("GET", result.action);
            Assert.assertEquals(false, result.dir);
        }
        {
            result = listing.get(2);
            Assert.assertEquals(key + "/f3", result.key);
            Assert.assertEquals("f3", result.value);
            Assert.assertEquals("GET", result.action);
            Assert.assertEquals(false, result.dir);
        }
        {
            result = listing.get(3);
            Assert.assertEquals(key + "/subdir1", result.key);
            Assert.assertEquals(null, result.value);
            Assert.assertEquals("GET", result.action);
            Assert.assertEquals(true, result.dir);
        }
    }

    @Test
    public void testGetVersion() throws Exception {
        String version = this.client.getVersion();
        Assert.assertTrue(version.startsWith("etcd v0."));
    }

}
