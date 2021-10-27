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

package org.apache.iceberg.dell.mock;

import com.emc.object.s3.S3Client;
import com.emc.object.s3.bean.ListObjectsResult;
import com.emc.object.s3.bean.ObjectKey;
import com.emc.object.s3.request.DeleteObjectsRequest;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.dell.EcsClientFactory;
import org.apache.iceberg.dell.EcsClientProperties;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Mock rule of ECS S3 mock.
 * <p>
 * Use environment parameter to specify use mock client or real client.
 */
public class EcsS3MockRule implements TestRule {

  // Config fields
  private final boolean autoCreateBucket;

  // Setup fields
  private Map<String, String> clientProperties;
  private String bucket;
  private boolean mock;

  // State fields during test
  /**
   * Lazy client for test rule.
   */
  private S3Client lazyClient;
  private boolean bucketCreated;

  public static EcsS3MockRule create() {
    return new EcsS3MockRule(true);
  }

  public static EcsS3MockRule manualCreateBucket() {
    return new EcsS3MockRule(false);
  }

  public EcsS3MockRule(boolean autoCreateBucket) {
    this.autoCreateBucket = autoCreateBucket;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        initialize();
        try {
          base.evaluate();
        } finally {
          cleanUp();
        }
      }
    };
  }

  private void initialize() {
    if (System.getenv(EcsClientProperties.ENDPOINT) == null) {
      clientProperties = MockEcsClientFactory.MOCK_ECS_CLIENT_PROPERTIES;
      bucket = "test";
      mock = true;
    } else {
      Map<String, String> properties = new LinkedHashMap<>();
      properties.put(EcsClientProperties.ACCESS_KEY_ID, System.getenv(EcsClientProperties.ACCESS_KEY_ID));
      properties.put(EcsClientProperties.SECRET_ACCESS_KEY, System.getenv(EcsClientProperties.SECRET_ACCESS_KEY));
      properties.put(EcsClientProperties.ENDPOINT, System.getenv(EcsClientProperties.ENDPOINT));
      clientProperties = properties;
      bucket = "test-" + UUID.randomUUID();
      if (autoCreateBucket) {
        createBucket();
      }

      mock = false;
    }
  }

  private void cleanUp() {
    if (mock) {
      S3Client client = this.lazyClient;
      if (client != null) {
        client.destroy();
      }
    } else {
      S3Client client = getClient();
      if (bucketCreated) {
        deleteBucket();
      }

      client.destroy();
    }
  }

  public void createBucket() {
    // create test bucket for this unit test
    getClient().createBucket(bucket);
    bucketCreated = true;
  }

  private void deleteBucket() {
    S3Client client = getClient();
    if (!client.bucketExists(bucket)) {
      return;
    }

    // clean up test bucket of this unit test
    while (true) {
      ListObjectsResult result = client.listObjects(bucket);
      if (result.getObjects().isEmpty()) {
        break;
      }

      List<ObjectKey> keys = result.getObjects()
          .stream()
          .map(it -> new ObjectKey(it.getKey()))
          .collect(Collectors.toList());
      client.deleteObjects(new DeleteObjectsRequest(bucket).withKeys(keys));
    }

    client.deleteBucket(bucket);
  }

  public Map<String, String> getClientProperties() {
    return clientProperties;
  }

  public S3Client getClient() {
    if (lazyClient == null) {
      lazyClient = EcsClientFactory.create(clientProperties);
    }

    return lazyClient;
  }

  public String getBucket() {
    return bucket;
  }
}