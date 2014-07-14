package com.justinsb.etcd;

import java.io.IOException;

public class EtcdClientException extends IOException {
  private static final long serialVersionUID = 1L;

  final Integer httpStatusCode;
  final EtcdResult result;

  public EtcdClientException(final String message, final Throwable cause) {
    super(message, cause);
    this.httpStatusCode = null;
    this.result = null;
  }

  public EtcdClientException(final String message, final int httpStatusCode) {
    super(message);
    this.httpStatusCode = httpStatusCode;
    this.result = null;
  }

  public EtcdClientException(final String message, final EtcdResult result) {
    super(message);
    this.httpStatusCode = null;
    this.result = result;
  }

  public boolean isHttpError(final int newHttpStatusCode) {
    return (this.httpStatusCode != null && newHttpStatusCode == this.httpStatusCode);
  }

  public boolean isEtcdError(final int etcdCode) {
    return (this.result != null && this.result.errorCode != null && etcdCode == this.result.errorCode);
  }
}