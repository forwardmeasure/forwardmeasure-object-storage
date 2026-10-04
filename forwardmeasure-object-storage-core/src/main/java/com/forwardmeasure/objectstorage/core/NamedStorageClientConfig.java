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
package com.forwardmeasure.objectstorage.core;

import java.util.Map;
import java.util.Optional;

public interface NamedStorageClientConfig {

  String backend();

  Optional<String> bucket();

  Optional<String> endpoint();

  /**
   * Browser-visible endpoint used when generating delegated capabilities. This may differ from
   * {@link #endpoint()} when applications access object storage through an in-cluster service name
   * but return capabilities to an external caller.
   */
  Optional<String> publicEndpoint();

  Optional<String> region();

  Optional<String> accessKey();

  Optional<String> secretKey();

  Optional<Boolean> pathStyleAccess();

  Map<String, String> properties();
}
