package com.justinsb.etcd;

import java.util.List;

public class EtcdResult {
    // General values
    public String action;
    public String key;
    public String value;
    public long index;
    public EtcdResult node;

    // For set operations
    public String prevValue;
    public boolean newKey;

    // For TTL keys
    public String expiration;
    public Integer ttl;

    // For listings
    public boolean dir;
    public List<EtcdResult> nodes;

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
