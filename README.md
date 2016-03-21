## jetcd: Java binding for etcd

TravisCI: [![Build Status](https://travis-ci.org/mlaccetti/jetcd.png?branch=master)](https://travis-ci.org/mlaccetti/jetcd)

CircleCI: ![CircleCI Status](https://circleci.com/gh/mlaccetti/jetcd.png?circle-token=e3cd8a7b12658d90965d7491b48dbde2fad76ecd)


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

For a bit of background, check out the [blog post].

### Testing

1. Export the IP for the node to use (this example is the typical IP of a `docker-machine` host:
    ```
    export HOST_IP="192.168.99.100"
    ```
1. Fire up an `etcd` Docker container:

    ```
  docker run -d -p 4001:4001 -p 2380:2380 -p 2379:2379 --name etcd quay.io/coreos/etcd:v2.0.3 \
   -name etcd0 \
   -advertise-client-urls http://${HOST_IP}:2379,http://${HOST_IP}:4001 \
   -listen-client-urls http://0.0.0.0:2379,http://0.0.0.0:4001 \
   -initial-advertise-peer-urls http://${HOST_IP}:2380 \
   -listen-peer-urls http://0.0.0.0:2380 \
   -initial-cluster-token etcd-cluster-1 \
   -initial-cluster etcd0=http://${HOST_IP}:2380 \
   -initial-cluster-state new
    ```


[blog post]: http://blog.justinsb.com
[etcd]: http://coreos.com/blog/distributed-configuration-with-etcd/
[SmokeTest.java]: https://github.com/justinsb/jetcd/blob/master/src/test/java/com/justinsb/etcd/SmokeTest.java
[ListenableFuture]: https://code.google.com/p/guava-libraries/wiki/ListenableFutureExplained
[Guava]: https://plus.google.com/118010414872916542489
[HttpAsyncClient]:http://hc.apache.org/httpcomponents-asyncclient-dev/

