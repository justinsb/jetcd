jetcd: Java binding for etcd
============================

A Java binding for the awesome etcd

Use the Apache HttpAsyncClient to implement watches without blocking a thread.

Check out SmokeTest.java to see how this is used (and tested), but here's a quick code example:

```Java
EtcdClient client = new EtcdClient(URI.create("http://127.0.0.1:4001/"));

String key = "/watch";

EtcdResult result = this.client.set(key, "hello");
Assert.assertEquals("hello", result.value);

result = this.client.get(key);
Assert.assertEquals("hello", result.value);
        
ListenableFuture<EtcdResult> watchFuture = this.client.watch(key, result.index + 1);
Assert.assertFalse(watchFuture.isDone());

result = this.client.set(key, "world");
Assert.assertEquals("world", result.value);

EtcdResult watchResult = watchFuture.get(100, TimeUnit.MILLISECONDS);
Assert.assertNotNull(result);
Assert.assertEquals("world", result.value);

```
 
For a bit of background, check out the [blog post]


[blog post]: http://blog.justinsb.com



