package com.justinsb.etcd;

import java.io.IOException;

public class EtcdClientException extends IOException {
  private static final long serialVersionUID = 1L;

  final Integer             httpStatusCode;

  final EtcdResult          result;

  public EtcdClientException(String message, Throwable cause) {
    super(message, cause);
    this.httpStatusCode = null;
    this.result = null;
  }

  public EtcdClientException(String message, int httpStatusCode) {
    super(message);
    this.httpStatusCode = httpStatusCode;
    this.result = null;
  }

  public EtcdClientException(String message, EtcdResult result) {
    super(message);
    this.httpStatusCode = null;
    this.result = result;
  }

  public boolean isHttpError(int newHttpStatusCode) {
    return (this.httpStatusCode != null && newHttpStatusCode == this.httpStatusCode);
  }

  public boolean isEtcdError(int etcdCode) {
    return (this.result != null && this.result.errorCode != null && etcdCode == this.result.errorCode);

  }
}