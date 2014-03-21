package com.justinsb.etcd;

public class EtcdResult {
	// General values
	public String action;
	public EtcdNode node;
	public EtcdNode prevNode;

	// For errors
	public Integer errorCode;
	public String message;
	public String cause;
	public int errorIndex;

	// Server metadata
	public long etcdIndex;
	public long raftIndex;
	public long raftTerm;

	public boolean isError() {
		return errorCode != null;
	}

	@Override
	public String toString() {
		return EtcdClient.format(this);
	}
}
