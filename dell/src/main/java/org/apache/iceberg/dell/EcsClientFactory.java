/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.dell;

import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.jersey.S3JerseyClient;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.net.URI;
import java.util.Map;
import java.util.Optional;

public interface EcsClientFactory {

  /**
   * Create the ECS S3 Client from properties
   */
  static S3Client create(Map<String, String> properties) {
    return createWithFactory(properties).orElseGet(() -> createDefault(properties));
  }

  /**
   * Try to create the ECS S3 client from factory method.
   */
  static Optional<S3Client> createWithFactory(Map<String, String> properties) {
    String factory = properties.get(EcsCatalogProperties.ECS_CLIENT_FACTORY);
    if (factory == null || factory.isEmpty()) {
      return Optional.empty();
    }
    String[] classAndMethod = factory.split("#", 2);
    if (classAndMethod.length != 2) {
      throw new IllegalArgumentException(String.format("invalid property %s", EcsCatalogProperties.ECS_CLIENT_FACTORY));
    }
    Class<?> clazz;
    try {
      clazz = Class.forName(classAndMethod[0], true, Thread.currentThread().getContextClassLoader());
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          String.format("invalid property %s", EcsCatalogProperties.ECS_CLIENT_FACTORY),
          e);
    }
    S3Client client;
    try {
      client = (S3Client) MethodHandles.lookup()
          .findStatic(clazz, classAndMethod[1], MethodType.methodType(S3Client.class, Map.class))
          .invoke(properties);
    } catch (Throwable e) {
      throw new IllegalArgumentException(
          String.format("invalid property %s that throw exception", EcsCatalogProperties.ECS_CLIENT_FACTORY),
          e);
    }
    if (client == null) {
      throw new IllegalArgumentException(String.format(
          "invalid property %s that return null client",
          EcsCatalogProperties.ECS_CLIENT_FACTORY));
    }
    return Optional.of(client);
  }

  /**
   * Get built-in ECS S3 client.
   */
  static S3Client createDefault(Map<String, String> properties) {
    S3Config config = new S3Config(URI.create(properties.get(EcsCatalogProperties.ENDPOINT)));

    config.withIdentity(properties.get(EcsCatalogProperties.ACCESS_KEY_ID))
        .withSecretKey(properties.get(EcsCatalogProperties.SECRET_ACCESS_KEY));

    return new S3JerseyClient(config);
  }
}
