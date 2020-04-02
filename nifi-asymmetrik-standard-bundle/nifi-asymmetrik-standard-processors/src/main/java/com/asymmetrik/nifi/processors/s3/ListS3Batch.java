package com.asymmetrik.nifi.processors.s3;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;


@PrimaryNodeOnly
@TriggerWhenEmpty
@SupportsBatching
@Tags({"s3", "list", "AWS"})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Lists specified files from s3 gradually, unlike the ListS3 processor. Specification consists of a bucket, and " + 
    "optional prefix, start key and end key. Only allows the use of default AWS credentials."
    )
@Stateful(description = "Stores the key of the last file listed by the processor, the completion state and the latest configuration.", scopes = {Scope.CLUSTER})
// @formatter:off
@WritesAttributes({
    @WritesAttribute(attribute = "s3.bucket",       description = "The name of the S3 bucket"),
    @WritesAttribute(attribute = "filename",        description = "The name of the file"),
    @WritesAttribute(attribute = "s3.etag",         description = "The ETag that can be used to see if the file has changed"),
    @WritesAttribute(attribute = "s3.lastModified", description = "The last modified time in milliseconds since epoch in UTC time"),
    @WritesAttribute(attribute = "s3.size",         description = "The size of the object in bytes"),
    @WritesAttribute(attribute = "s3.storeClass",   description = "The storage class of the object")})
// @formatter:on
public class ListS3Batch extends AbstractProcessor {
    
    @VisibleForTesting
    static final String FILENAME_ATTRIBUTE = CoreAttributes.FILENAME.key();

    @VisibleForTesting
    static final String S3_BUCKET_ATTRIBUTE = "s3.bucket";

    @VisibleForTesting
    static final String S3_ETAG_ATTRIBUTE = "s3.etag";

    @VisibleForTesting
    static final String LAST_MODIFIED_ATTRIBUTE = "s3.lastModified";

    @VisibleForTesting
    static final String S3_SIZE_ATTRIBUTE = "s3.size";

    @VisibleForTesting
    static final String S3_STORECLASS_ATTRIBUTE = "s3.storeClass";

    @VisibleForTesting
    static final String LAST_ELEMENT_KEY = "lastElement";
    
    @VisibleForTesting
    static final String STARTED_AT = "startedAt";
    
    @VisibleForTesting
    static final String COMPLETED_AT = "completedAt";
    
    @VisibleForTesting
    static final String LAST_CONFIG = "lastConfig";
    
    @VisibleForTesting
    static final String OBJECTS_PROCESSED = "objectsProcessed";
    
    public static final PropertyDescriptor S3_BUCKET = new PropertyDescriptor.Builder()
            .name("S3 Bucket")
            .description("S3 bucket to list objects from.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor S3_PREFIX = new PropertyDescriptor.Builder()
            .name("File S3 Prefix")
            .description("S3 prefix to list files from. Defaults to the root of the bucket.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .addValidator(Validator.VALID)
            .defaultValue("")
            .build();
    
    public static final PropertyDescriptor S3_START_PREFIX = new PropertyDescriptor.Builder()
            .name("S3 Start Prefix")
            .description("Subprefix under File S3 Prefix marking the inclusive start point for listing files. "
                    + "Note that because of the way S3 keys work this will be exclusive of the specified object "
                    + "if you use a full object key instead of a prefix.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .addValidator(Validator.VALID)
            .defaultValue("")
            .build();
    
    public static final PropertyDescriptor S3_END_PREFIX = new PropertyDescriptor.Builder()
            .name("S3 End Prefix")
            .description("Subprefix under File S3 Prefix marking the exclusive end point for listing files.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .addValidator(Validator.VALID)
            .defaultValue("")
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Max S3 Keys Per Batch")
            .description("Maximum number of keys to fetch per execution of ListS3. Highest allowable value is 1000.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .addValidator(StandardValidators.createLongValidator(1, 1000, true))
            .defaultValue("100")
            .build();

    static final PropertyDescriptor FETCH_OWNER = new PropertyDescriptor.Builder()
            .name("Fetch S3 Object's Owner")
            .description("Determines whether or not to return the owner of each object when fetching from s3.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor AWS_REGION = new PropertyDescriptor.Builder()
            .name("AWS region containing the S3 bucket")
            .description("Determines which region that the S3 bucket is in. Default is us-east-1.")
            .allowableValues(getRegions())
            .defaultValue("us-east-1")
            .required(true)
            .build();

    static final Relationship SUCCESS = new Relationship.Builder()
            .description("All successfully read S3 objects get transferred to this relationship.")
            .name("success")
            .build();
    
    @Override
    public Set<Relationship> getRelationships() {
        return ImmutableSet.of(SUCCESS);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(S3_BUCKET, S3_PREFIX, S3_START_PREFIX, S3_END_PREFIX, BATCH_SIZE, FETCH_OWNER, AWS_REGION);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        final String region = context.getProperty(AWS_REGION).getValue();
        final String bucketName  = context.getProperty(S3_BUCKET)
                .evaluateAttributeExpressions()
                .getValue();
        final String prefix = context.getProperty(S3_PREFIX)
                .evaluateAttributeExpressions()
                .getValue();
        final String startKey = context.getProperty(S3_START_PREFIX)
                .evaluateAttributeExpressions()
                .getValue();
        final String endKey = context.getProperty(S3_END_PREFIX)
                .evaluateAttributeExpressions()
                .getValue();
        final boolean fetchOwner = context.getProperty(FETCH_OWNER).asBoolean();
        final String lastConfig = getItemFromContext(context,LAST_CONFIG);
        
        final String config = region + "," + bucketName + "," + prefix + "," + startKey + "," + endKey + "," + fetchOwner;
        
        String startAfter = null;
        
        // if the configuration properties have not changed, check our completion status and current position:
        if (config.equals(lastConfig)) {
            // Yield if the current listing is complete
            if (isComplete(context)) {
                context.yield();
                return;
            }
            else {
                // Find where we left off in any prior iteration
                startAfter = getItemFromContext(context,LAST_ELEMENT_KEY);
            }
        }
        
        // Create the request
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(prefix)
                .maxKeys(context.getProperty(BATCH_SIZE)
                        .evaluateAttributeExpressions()
                        .asInteger())
                .fetchOwner(context.getProperty(FETCH_OWNER).asBoolean())
                .build();
        
        if (startAfter == null) {
            startAfter = prefix + startKey;
        }
        request = request.toBuilder()
                .startAfter(startAfter)
                .build();

        // Execute the listS3 request
        final S3Client client = getClient(context);
        final ListObjectsV2Response listResponse = client.listObjectsV2(request);
        List<S3Object> s3obj = listResponse.contents();
        
        if (s3obj.isEmpty()) {
            persistState(context, null, config, true, 0);
            return;
        }
        
        boolean isDone = !listResponse.isTruncated();
        final String lastObjectKey = getLastObjectKey(s3obj);
        
        if (!endKey.isEmpty()) {
            final String fullEndKey = prefix + endKey;
            if(fullEndKey.compareTo(lastObjectKey) <= 0) {
                s3obj = s3obj.stream()
                        .filter((x) -> x.key().compareTo(fullEndKey) < 0)
                        .collect(Collectors.toList());
                isDone = true;
            }
        }
        
        if (s3obj.isEmpty()) {
            persistState(context, null, config, true, 0);
            return;
        }
        
        // For each response value, create a FlowFile with the received metadata, and gather them into a list
        final List<FlowFile> flowFiles = s3obj.stream()
                .map(obj -> s3ObjectToFlowFile(obj, session, bucketName, fetchOwner))
                .collect(Collectors.toList());

        // Send all the FlowFiles to the success relationship
        session.transfer(flowFiles, SUCCESS);

        // Persist the processor state by saving the last object key, whether to continue listing files for the current prefix, and the control file contents.
        persistState(context, getLastObjectKey(s3obj), config, isDone, s3obj.size());
    }

    /**
     * Function to store processor state.
     * If isComplete is true, then the processor also persists the current time.
     * @param context The processor context
     * @param objectKey The key to store in the processor's state.
     * @param isComplete Whether the processor is done listing files. If true, it adds a flag to stop listing additional files.
     */
    private void persistState(final ProcessContext context, String lastKey, String config, boolean isComplete, int objProcessed) {
        try {
            Map<String, String> state = new HashMap<>();
            String lastConfig = getItemFromContext(context, LAST_CONFIG);
            if (!config.equals(lastConfig)) {
                state.put(STARTED_AT, Instant.now().toString());
            }
            else {
                state.put(STARTED_AT, getItemFromContext(context,STARTED_AT));
            }
            if (isComplete) {
                state.put(COMPLETED_AT, Instant.now().toString());
            }
            state.put(LAST_ELEMENT_KEY, lastKey);
            state.put(LAST_CONFIG, config);
            String strProcessed = getItemFromContext(context,OBJECTS_PROCESSED);
            int alreadyProcessed = strProcessed == null ? 0 : Integer.parseInt(strProcessed);
            state.put(OBJECTS_PROCESSED, String.valueOf(objProcessed + alreadyProcessed));
            context.getStateManager().setState(state, Scope.CLUSTER);
        } catch (IOException ioe) {
            getLogger().error("Failed to save local state. If NiFi is restarted, data duplication may occur", ioe);
        }
    }
    

    /**
     * Function to convert an S3Object to a FlowFile.
     * @param obj The S3Object to convert to a FlowFile
     * @param session The processor's ProcessSession
     * @param bucketName The bucket that the file metadata was fetched from
     * @param fetchOwner Whether or not the owner was fetched by the listS3 request
     * @return FlowFile with attributes containing the s3 object's metadata, as defined in the processor's header.
     */
    private FlowFile s3ObjectToFlowFile(S3Object obj, ProcessSession session, String bucketName, boolean fetchOwner) {

        final Map<String, String> attributes = new HashMap<>();
        
        attributes.put(FILENAME_ATTRIBUTE, obj.key());
        attributes.put(S3_BUCKET_ATTRIBUTE, bucketName);
        attributes.put(S3_ETAG_ATTRIBUTE, obj.eTag());
        attributes.put(LAST_MODIFIED_ATTRIBUTE, String.valueOf(obj.lastModified()));
        attributes.put(S3_SIZE_ATTRIBUTE, String.valueOf(obj.size()));
        attributes.put(S3_STORECLASS_ATTRIBUTE, obj.storageClassAsString());

        if (fetchOwner) {
            attributes.put("s3.owner", obj.owner().id());
        }

        return session.putAllAttributes(session.create(), attributes);
    }
    
    /**
     * Gets the key of the last object from a list of S3 objects.
     */
    private String getLastObjectKey(List<S3Object> objects) {
        return objects.get(objects.size() - 1).key();
    }
    
    private String getItemFromContext(ProcessContext context, String item) throws ProcessException {
        try {
            StateMap stateMap = context.getStateManager().getState(Scope.CLUSTER);
            return stateMap.get(item);
        } catch (IOException ioe) {
            throw new ProcessException(ioe);
        }
    }

    private boolean isComplete(ProcessContext context) throws ProcessException {
        try {
            StateMap stateMap = context.getStateManager().getState(Scope.CLUSTER);
            return stateMap.get(COMPLETED_AT) != null;
        } catch (IOException ioe) {
            throw new ProcessException(ioe);
        }
    }
    
    private static Set<String> getRegions() {
        return Region.regions().stream()
                .map(region -> region.id())
                .collect(Collectors.toSet());
    }
    
    protected S3Client getClient(ProcessContext context) {
        return S3Client.builder()
                .region(Region.of(context.getProperty(AWS_REGION).getValue()))
                .build();
    }
}
