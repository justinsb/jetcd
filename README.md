jetcd: Java binding for etcd
============================

TravisCI: [![Build Status](https://travis-ci.org/justinsb/jetcd.png?branch=master)](https://travis-ci.org/justinsb/jetcd)

CircleCI: ![CircleCI Status](https://circleci.com/gh/justinsb/jetcd.png?circle-token=ebf4870e1fc43b6d6139a0514312441b7dc11457)


A simple Java client library for the awesome [etcd]

Uses the Apache [HttpAsyncClient] to implement watches without blocking a thread, and Google's [Guava] to give us the nice [ListenableFuture] interface. 

Check out [SmokeTest.java] to see how this is used (and tested), but here's a quick code example:

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
[etcd]: http://coreos.com/blog/distributed-configuration-with-etcd/
[SmokeTest.java]: https://github.com/justinsb/jetcd/blob/master/src/test/java/com/justinsb/etcd/SmokeTest.java
[ListenableFuture]: https://code.google.com/p/guava-libraries/wiki/ListenableFutureExplained
[Guava]: https://plus.google.com/118010414872916542489
[HttpAsyncClient]:http://hc.apache.org/httpcomponents-asyncclient-dev/

