package com.justinsb.etcd;

public class EtcdResult {
    // General values
    public String action;
    public String key;
    public String value;
    public long index;

    // For set operations
    public String prevValue;
    public boolean newKey;

    // For TTL keys
    public String expiration;
    public Integer ttl;

    // For listings
    public boolean dir;

    // For errors
    public Integer errorCode;
    public String message;
    public String cause;

    public boolean isError() {
        return errorCode != null;
    }

    @Override
    public String toString() {
        return EtcdClient.format(this);
    }
}
