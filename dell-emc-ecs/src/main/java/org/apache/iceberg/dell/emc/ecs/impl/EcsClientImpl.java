/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.dell.emc.ecs.impl;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.iceberg.dell.emc.ecs.EcsClient;
import org.apache.iceberg.dell.emc.ecs.ObjectHeadInfo;
import org.apache.iceberg.dell.emc.ecs.ObjectKey;
import org.apache.iceberg.dell.emc.ecs.ObjectKeys;

/**
 * the implementation of {@link EcsClient}
 * <p>
 * ECS use aws sdk v1 to support private function.
 */
public class EcsClientImpl implements EcsClient {

    private final AmazonS3 s3;
    private final Map<String, String> properties;
    private final ObjectKeys keys;

    public EcsClientImpl(AmazonS3 s3, Map<String, String> properties, ObjectKeys keys) {
        this.s3 = s3;
        this.properties = properties;
        this.keys = keys;
    }

    @Override
    public ObjectKeys getKeys() {
        return keys;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public Optional<ObjectHeadInfo> head(ObjectKey key) {
        try {
            ObjectMetadata metadata = s3.getObjectMetadata(key.getBucket(), key.getKey());
            return Optional.of(new ObjectHeadInfoImpl(metadata.getContentLength(), metadata.getETag()));
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                return Optional.empty();
            } else {
                throw e;
            }
        }
    }

    @Override
    public InputStream inputStream(ObjectKey key, long pos) {
        S3Object object = s3.getObject(new GetObjectRequest(key.getBucket(), key.getKey())
                .withRange(pos));
        return object.getObjectContent();
    }

    @Override
    public OutputStream outputStream(ObjectKey key) {
        return new EcsAppendOutputStream(s3, key, new byte[1_000]);
    }

    @Override
    public ContentAndETag readAll(ObjectKey key) {
        S3Object object = s3.getObject(new GetObjectRequest(key.getBucket(), key.getKey()));
        int size = (int) object.getObjectMetadata().getContentLength();
        byte[] content = new byte[size];
        try (S3ObjectInputStream input = object.getObjectContent()) {
            int offset = 0;
            while (offset < size) {
                offset += input.read(content, offset, size - offset);
            }
            if (offset != size) {
                throw new IllegalStateException(String.format(
                        "size of %s is unmatched, current size %d != %d",
                        key, offset, size));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("rethrow unchecked exception during read all bytes", e);
        }
        return new ContentAndETagImpl(object.getObjectMetadata().getETag(), content);
    }

    /**
     * using If-Match to replace object with eTag
     *
     * @param key   is object key
     * @param eTag  is e-tag
     * @param bytes is content
     * @return true if replace success
     */
    @Override
    public boolean replace(ObjectKey key, String eTag, byte[] bytes) {
        return cas(() -> {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setHeader("If-Match", eTag);
            metadata.setContentLength(bytes.length);
            s3.putObject(key.getBucket(), key.getKey(), new ByteArrayInputStream(bytes), metadata);
        });
    }

    /**
     * using If-None-Match to write object
     *
     * @param key   is object key
     * @param bytes is content
     * @return true if object is absent when write object
     */
    @Override
    public boolean writeIfAbsent(ObjectKey key, byte[] bytes) {
        return cas(() -> {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setHeader("If-None-Match", "*");
            metadata.setContentLength(bytes.length);
            s3.putObject(key.getBucket(), key.getKey(), new ByteArrayInputStream(bytes), metadata);
        });
    }

    /**
     * using x-amz-copy-source-if-match and If-None-Match to copy object
     *
     * @param fromKey is source key
     * @param eTag    is E-Tag of source key
     * @param toKey   is destination key
     * @return true if object is copied to destination
     */
    @Override
    public boolean copyObjectIfAbsent(ObjectKey fromKey, String eTag, ObjectKey toKey) {
        return cas(() -> {
            CopyObjectRequest request = new CopyObjectRequest(fromKey.getBucket(), fromKey.getKey(), toKey.getBucket(), toKey.getKey());
            request.setMatchingETagConstraints(Collections.singletonList(eTag));
            request.putCustomRequestHeader("If-None-Match", "*");
            s3.copyObject(request);
        });
    }

    /**
     * cas error code
     *
     * @param fn is function that sending request
     * @return true if cas operation succeed
     */
    private boolean cas(Runnable fn) {
        try {
            fn.run();
            return true;
        } catch (AmazonS3Exception e) {
            if ("PreconditionFailed".equals(e.getErrorCode())) {
                return false;
            } else {
                throw e;
            }
        }
    }

    @Override
    public void deleteObject(ObjectKey key) {
        s3.deleteObject(key.getBucket(), key.getKey());
    }

    @Override
    public <T> List<T> listDelimiterAll(ObjectKey prefix, Function<ObjectKey, Optional<T>> filterAndMapper) {
        String delimiter = getKeys().getDelimiter();
        List<T> result = new ArrayList<>();
        String prefixKey;
        if (prefix.getKey().isEmpty()) {
            prefixKey = "";
        } else if (prefix.getKey().endsWith(delimiter)) {
            prefixKey = prefix.getKey();
        } else {
            prefixKey = prefix.getKey() + delimiter;
        }
        String continuationToken = null;
        do {
            ListObjectsV2Result response = s3.listObjectsV2(
                    new ListObjectsV2Request()
                            .withDelimiter(delimiter)
                            .withPrefix(prefixKey)
                            .withContinuationToken(continuationToken));
            continuationToken = response.getNextContinuationToken();
            for (S3ObjectSummary objectSummary : response.getObjectSummaries()) {
                Optional<T> itemOpt = filterAndMapper.apply(new ObjectKey(objectSummary.getBucketName(), objectSummary.getKey()));
                if (!itemOpt.isPresent()) {
                    continue;
                }
                result.add(itemOpt.get());
            }
        } while (continuationToken != null);
        return result;
    }

    @Override
    public void close() {
    }
}
