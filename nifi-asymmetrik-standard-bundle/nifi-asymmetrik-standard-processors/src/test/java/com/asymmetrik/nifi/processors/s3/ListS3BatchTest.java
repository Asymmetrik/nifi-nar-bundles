package com.asymmetrik.nifi.processors.s3;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static com.asymmetrik.nifi.processors.s3.ListS3Batch.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;



public class ListS3BatchTest {

    private TestRunner runner;

    private S3Client mockClient = Mockito.mock(S3Client.class);

    private static final String BUCKET_NAME = "test-bucket";

    private static final S3Object OBJECT1 = S3Object.builder()
            .size(1L)
            .key("test1")
            .eTag("tag1")
            .storageClass("STANDARD")
            .lastModified(Instant.EPOCH)
            .build();

    private static final S3Object OBJECT2 = S3Object.builder()
            .size(2L)
            .key("test2")
            .eTag("tag2")
            .storageClass("REDUCED_REDUNDANCY")
            .lastModified(Instant.MAX)
            .build();

    private static final ListObjectsV2Response RESPONSE1 = ListObjectsV2Response.builder()
            .contents(ImmutableList.of(OBJECT1))
            .isTruncated(true)
            .build();

    private static final ListObjectsV2Response RESPONSE2 = ListObjectsV2Response.builder()
            .contents(ImmutableList.of(OBJECT2))
            .isTruncated(false)
            .build();

    @Before
    public void setUp() {
        
        runner = TestRunners.newTestRunner(new ListS3Batch() {
            protected S3Client getClient(ProcessContext context) {
                return mockClient;
            }
        });

        runner.setValidateExpressionUsage(false);

        runner.setProperty(BATCH_SIZE, "1");
        runner.setProperty(S3_BUCKET, BUCKET_NAME);
    }

    @Test
    public void testLimitFlow() {

        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(RESPONSE1)
                .thenReturn(RESPONSE2);

        runner.run(1, false);
        runner.assertTransferCount(SUCCESS, 1);
        runner.getStateManager().assertStateEquals(OBJECTS_PROCESSED, "1", Scope.CLUSTER);
        runner.getStateManager().assertStateEquals(LAST_ELEMENT_KEY, "test1", Scope.CLUSTER);
        runner.run(1, false);
        runner.assertTransferCount(SUCCESS, 2);
        runner.getStateManager().assertStateEquals(OBJECTS_PROCESSED, "2", Scope.CLUSTER);
        runner.getStateManager().assertStateEquals(LAST_ELEMENT_KEY, "test2", Scope.CLUSTER);
        runner.getStateManager().assertStateSet(COMPLETED_AT, Scope.CLUSTER);
        runner.run(1, false);
        runner.assertTransferCount(SUCCESS, 2);
        
        runner.getStateManager().assertStateEquals(OBJECTS_PROCESSED, "2", Scope.CLUSTER);
        runner.getStateManager().assertStateEquals(LAST_ELEMENT_KEY, "test2", Scope.CLUSTER);
        runner.getStateManager().assertStateSet(COMPLETED_AT, Scope.CLUSTER);
    }

    @Test
    public void testCorrectAttributesAdded() {

        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(RESPONSE1)
                .thenReturn(RESPONSE2);

        runner.run(1, false);
        runner.assertTransferCount(SUCCESS, 1);
        final MockFlowFile flowFile1 = runner.getFlowFilesForRelationship(SUCCESS).get(0);

        flowFile1.assertAttributeEquals(S3_BUCKET_ATTRIBUTE, BUCKET_NAME);
        flowFile1.assertAttributeEquals(FILENAME_ATTRIBUTE, "test1");
        flowFile1.assertAttributeEquals(S3_ETAG_ATTRIBUTE, "tag1");
        flowFile1.assertAttributeEquals(LAST_MODIFIED_ATTRIBUTE, Instant.EPOCH.toString());
        flowFile1.assertAttributeEquals(S3_SIZE_ATTRIBUTE, String.valueOf(1L));
        flowFile1.assertAttributeEquals(S3_STORECLASS_ATTRIBUTE, "STANDARD");

        runner.getStateManager().assertStateEquals(OBJECTS_PROCESSED, "1", Scope.CLUSTER);
        runner.getStateManager().assertStateEquals(LAST_ELEMENT_KEY, "test1", Scope.CLUSTER);

        runner.run(1, false);
        runner.assertTransferCount(SUCCESS, 2);
        final MockFlowFile flowFile2 = runner.getFlowFilesForRelationship(SUCCESS).get(1);

        flowFile2.assertAttributeEquals(S3_BUCKET_ATTRIBUTE, BUCKET_NAME);
        flowFile2.assertAttributeEquals(FILENAME_ATTRIBUTE, "test2");
        flowFile2.assertAttributeEquals(S3_ETAG_ATTRIBUTE, "tag2");
        flowFile2.assertAttributeEquals(LAST_MODIFIED_ATTRIBUTE, Instant.MAX.toString());
        flowFile2.assertAttributeEquals(S3_SIZE_ATTRIBUTE, String.valueOf(2L));
        flowFile2.assertAttributeEquals(S3_STORECLASS_ATTRIBUTE, "REDUCED_REDUNDANCY");

        runner.getStateManager().assertStateEquals(OBJECTS_PROCESSED, "2", Scope.CLUSTER);
        runner.getStateManager().assertStateEquals(LAST_ELEMENT_KEY, "test2", Scope.CLUSTER);
        runner.getStateManager().assertStateSet(COMPLETED_AT, Scope.CLUSTER);

        runner.run(1, false);
        runner.assertTransferCount(SUCCESS, 2);

        runner.getStateManager().assertStateEquals(OBJECTS_PROCESSED, "2", Scope.CLUSTER);
        runner.getStateManager().assertStateEquals(LAST_ELEMENT_KEY, "test2", Scope.CLUSTER);
        runner.getStateManager().assertStateSet(COMPLETED_AT, Scope.CLUSTER);
    }
    
    
    @Test
    public void testEndKey() {
        
        final int batchSize = 3;
        
        runner.setProperty(S3_PREFIX, "one/abc/");
        runner.setProperty(S3_END_PREFIX, "03/A");
        runner.setProperty(BATCH_SIZE, String.valueOf(batchSize));
        

        final List<S3Object> respS3Obj = makeS3Objects(ImmutableList.of("one/"), ImmutableList.of("abc/"), ImmutableList.of("01/","02/","03/","04/"), ImmutableList.of("A","B"));
        final List<ListObjectsV2Response> responses = makeResponses(respS3Obj, batchSize);
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class)))
            .thenReturn(responses.get(0), responses.subList(1,responses.size()).toArray(new ListObjectsV2Response[0]));

        for (int i = 0; i < responses.size(); ++i) {
            runner.run();
        }
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(SUCCESS);
        final List<String> actKeys = flowFiles.stream().map((x) -> x.getAttributes().get("filename")).collect(Collectors.toList());
        
        final List<S3Object> expS3Obj = makeS3Objects(ImmutableList.of("one/"), ImmutableList.of("abc/"), ImmutableList.of("01/","02/"), ImmutableList.of("A","B"));
        final List<String> expKeys = expS3Obj.stream().map((x) -> x.key()).collect(Collectors.toList());
        assertEquals(expKeys, actKeys);
        runner.getStateManager().assertStateEquals(OBJECTS_PROCESSED, "4", Scope.CLUSTER);
        runner.getStateManager().assertStateSet(COMPLETED_AT, Scope.CLUSTER);
    }
    
    @Test
    public void testStartKey() {
        
        final int batchSize = 3;
        
        runner.setProperty(S3_PREFIX, "one/abc/");
        runner.setProperty(S3_START_PREFIX, "02/B");
        runner.setProperty(BATCH_SIZE, String.valueOf(batchSize));
        
        final List<S3Object> allS3Obj = makeS3Objects(ImmutableList.of("one/"), ImmutableList.of("abc/"), ImmutableList.of("01/","02/","03/","04/"), ImmutableList.of("A","B"));
        setupMockClient(allS3Obj, batchSize);
        for (int i = 0; i < allS3Obj.size(); ++i) {
            runner.run();
        }
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(SUCCESS);
        final List<String> actKeys = flowFiles.stream().map((x) -> x.getAttributes().get("filename")).collect(Collectors.toList());
        
        List<S3Object> expS3Obj = allS3Obj.stream().filter((x) -> "one/abc/02/B".compareTo(x.key()) < 0).collect(Collectors.toList());
        final List<String> expKeys = expS3Obj.stream().map((x) -> x.key()).collect(Collectors.toList());
        assertEquals(expKeys, actKeys);
        runner.getStateManager().assertStateEquals(OBJECTS_PROCESSED, "4", Scope.CLUSTER);
        runner.getStateManager().assertStateSet(COMPLETED_AT, Scope.CLUSTER);
    }
    
    @Test
    public void testStartAndEndKey() {
        
        final int batchSize = 3;
        
        runner.setProperty(S3_PREFIX, "one/abc/");
        runner.setProperty(S3_START_PREFIX, "01/B");
        runner.setProperty(S3_END_PREFIX, "03/B");
        runner.setProperty(BATCH_SIZE, String.valueOf(batchSize));
        
        final List<S3Object> allS3Obj = makeS3Objects(ImmutableList.of("one/"), ImmutableList.of("abc/"), ImmutableList.of("01/","02/","03/","04/"), ImmutableList.of("A","B"));
        setupMockClient(allS3Obj, batchSize);
        for (int i = 0; i < allS3Obj.size(); ++i) {
            runner.run();
        }
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(SUCCESS);
        final List<String> actKeys = flowFiles.stream().map((x) -> x.getAttributes().get("filename")).collect(Collectors.toList());
        
        List<S3Object> expS3Obj = allS3Obj.stream().filter((x) -> "one/abc/01/B".compareTo(x.key()) < 0 & x.key().compareTo("one/abc/03/B") < 0).collect(Collectors.toList());
        final List<String> expKeys = expS3Obj.stream().map((x) -> x.key()).collect(Collectors.toList());
        assertEquals(expKeys, actKeys);
        runner.getStateManager().assertStateEquals(OBJECTS_PROCESSED, "3", Scope.CLUSTER);
        runner.getStateManager().assertStateSet(COMPLETED_AT, Scope.CLUSTER);
    }
    
    private void setupMockClient(List<S3Object> allS3Obj, int batchSize) {
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class))).thenAnswer(
                invocation -> {
                    ListObjectsV2Request request = invocation.getArgument(0);
                    if (request.startAfter() != null) {
                        List<S3Object> afterS3Obj = allS3Obj.stream().filter((x) -> request.startAfter().compareTo(x.key()) < 0).collect(Collectors.toList());
                        if (afterS3Obj.isEmpty()) {
                            return ListObjectsV2Response.builder()
                                    .contents(new ArrayList<S3Object>())
                                    .isTruncated(false)
                                    .build();
                        }
                        else {
                            return makeResponses(afterS3Obj,batchSize).get(0);
                        }
                    }
                    else {
                        return makeResponses(allS3Obj,batchSize).get(0);
                    }
                }
            );
    }
    
    private List<S3Object> makeS3Objects(List<String> as, List<String> bs, List<String> cs, List<String> ds) {
        List<S3Object> out = new ArrayList<>();
        for (final String a : as) {
            for (final String b : bs) {
                for (final String c : cs) {
                    for (final String d : ds) {
                        final String key = a + b + c + d;
                        final S3Object o = S3Object.builder()
                            .size(1L)
                            .key(key)
                            .eTag("tag1")
                            .storageClass("STANDARD")
                            .lastModified(Instant.EPOCH)
                            .build();
                        out.add(o);
                    }
                }
            }
        }
        return out;
    }
    
    private List<ListObjectsV2Response> makeResponses(List<S3Object> s3obj, int batchSize) {
        final List<ListObjectsV2Response> out = new ArrayList<>();
        for(int i=0; i < s3obj.size(); i+=batchSize) {
            final List<S3Object> sub = s3obj.subList(i,Math.min(i+batchSize,s3obj.size()));
            final ListObjectsV2Response response = ListObjectsV2Response.builder()
                    .contents(sub)
                    .isTruncated(true)
                    .build();
            out.add(response);
        }
        return out;
    }
    
}
