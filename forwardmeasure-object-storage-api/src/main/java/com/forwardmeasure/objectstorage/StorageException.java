/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.forwardmeasure.objectstorage;

import java.util.Objects;

/**
 * Base exception for all storage client failures.
 *
 * <p>Design goals: - Runtime exception (idiomatic for SDK-style libraries). - Carries optional
 * provider name and an error code for programmatic handling. - Can wrap underlying SDK exceptions
 * without losing the causal chain.
 */
public class StorageException extends RuntimeException {

  private final String provider; // e.g., "s3", "gcs", "azure" (optional)
  private final String code; // e.g., "NOT_FOUND", "ACCESS_DENIED", "CONFLICT" (optional)
  private final boolean retryable; // hint for callers

  public StorageException(String message) {
    this(message, null, null, false, null);
  }

  public StorageException(String message, Throwable cause) {
    this(message, null, null, false, cause);
  }

  public StorageException(String message, String provider, String code, boolean retryable) {
    this(message, provider, code, retryable, null);
  }

  public StorageException(
      String message, String provider, String code, boolean retryable, Throwable cause) {
    super(Objects.requireNonNull(message, "message"), cause);
    this.provider = provider;
    this.code = code;
    this.retryable = retryable;
  }

  /**
   * @return Provider identifier if known (e.g. "s3", "gcs", "azure"), else null.
   */
  public String provider() {
    return provider;
  }

  /**
   * @return A stable, programmatic error code if known, else null.
   */
  public String code() {
    return code;
  }

  /**
   * @return True if the operation may succeed on retry (timeouts, throttling, transient network
   *     issues).
   */
  public boolean retryable() {
    return retryable;
  }

  // -------------------------
  // Convenience constructors
  // -------------------------

  public static StorageException notFound(String provider, String message, Throwable cause) {
    return new StorageException(message, provider, "NOT_FOUND", false, cause);
  }

  public static StorageException accessDenied(String provider, String message, Throwable cause) {
    return new StorageException(message, provider, "ACCESS_DENIED", false, cause);
  }

  public static StorageException conflict(String provider, String message, Throwable cause) {
    return new StorageException(message, provider, "CONFLICT", false, cause);
  }

  public static StorageException invalidRequest(String provider, String message, Throwable cause) {
    return new StorageException(message, provider, "INVALID_REQUEST", false, cause);
  }

  public static StorageException retryable(String provider, String message, Throwable cause) {
    return new StorageException(message, provider, "RETRYABLE", true, cause);
  }

  public static StorageException internal(String provider, String message, Throwable cause) {
    return new StorageException(message, provider, "INTERNAL", false, cause);
  }
}
