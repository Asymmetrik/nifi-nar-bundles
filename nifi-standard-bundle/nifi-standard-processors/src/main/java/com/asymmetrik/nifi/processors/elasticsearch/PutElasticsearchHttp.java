package com.asymmetrik.nifi.processors.elasticsearch;

import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.commons.lang3.StringUtils.trimToEmpty;


@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven
@SupportsBatching
@Tags({"elasticsearch", "insert", "update", "upsert", "delete", "write", "put", "http"})
@CapabilityDescription("Writes the contents of a FlowFile to Elasticsearch, using the specified parameters such as "
        + "the index to insert into and the type of the document.")
@DynamicProperty(
        name = "A URL query parameter",
        value = "The value to set it to",
        supportsExpressionLanguage = true,
        description = "Adds the specified property name/value as a query parameter in the Elasticsearch URL used for processing")
public class PutElasticsearchHttp extends AbstractElasticsearchHttpProcessor {

    private static final String DOC_AS_UPSERT = "doc_as_upsert";
    private static final String SCRIPTED_UPSERT = "scripted_upsert";

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are written to Elasticsearch are routed to this relationship").build();

    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be written to Elasticsearch are routed to this relationship").build();

    public static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
            .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
            .build();

    public static final PropertyDescriptor ID_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("put-es-id-attr")
            .displayName("Identifier Attribute")
            .description("The name of the FlowFile attribute containing the identifier for the document. If the Index Operation is \"index\", "
                    + "this property may be left empty or evaluate to an empty value, in which case the document's identifier will be "
                    + "auto-generated by Elasticsearch. For all other Index Operations, the attribute must evaluate to a non-empty value.")
            .required(false)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
            .name("put-es-index")
            .displayName("Index")
            .description("The name of the index to insert into")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(
                    AttributeExpression.ResultType.STRING, true))
            .build();

    public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
            .name("put-es-type")
            .displayName("Type")
            .description("The type of this document (used by Elasticsearch for indexing and searching)")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor INDEX_OP = new PropertyDescriptor.Builder()
            .name("put-es-index-op")
            .displayName("Index Operation")
            .description("The type of the operation used to index (index, update, upsert, delete)")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .defaultValue("index")
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("put-es-batch-size")
            .displayName("Batch Size")
            .description("The preferred number of flow files to put to the database in a single transaction. Note that the contents of the "
                    + "flow files will be stored in memory until the bulk operation is performed. Also the results should be returned in the "
                    + "same order the flow files were received.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor SCRIPT = new PropertyDescriptor.Builder()
            .name("script")
            .displayName("Script Object (JSON)")
            .description("Optional script object to include in request. This will be added to the http body only when " +
                    "Index Operation is \"update\" or \"upsert\".")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(
                    AttributeExpression.ResultType.STRING, true))
            .build();

    public static final PropertyDescriptor UPSERT_OPTION = new PropertyDescriptor.Builder()
            .name("upsert-option")
            .displayName("Upsert Option")
            .description("Optional upsert parameter that determines the structure of the http body only when " +
                    "Index Operation is \"update\" or \"upsert\". Choices include \"" + DOC_AS_UPSERT + "\" and \"" + SCRIPTED_UPSERT + "\".")
            .required(false)
            .allowableValues(DOC_AS_UPSERT, SCRIPTED_UPSERT)
            .build();


    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> propertyDescriptors;

    static {
        final Set<Relationship> _rels = new HashSet<>();
        _rels.add(REL_SUCCESS);
        _rels.add(REL_FAILURE);
        _rels.add(REL_RETRY);
        relationships = Collections.unmodifiableSet(_rels);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(ES_URL);
        descriptors.add(PROP_SSL_CONTEXT_SERVICE);
        descriptors.add(USERNAME);
        descriptors.add(PASSWORD);
        descriptors.add(CONNECT_TIMEOUT);
        descriptors.add(RESPONSE_TIMEOUT);
        descriptors.add(ID_ATTRIBUTE);
        descriptors.add(INDEX);
        descriptors.add(TYPE);
        descriptors.add(CHARSET);
        descriptors.add(BATCH_SIZE);
        descriptors.add(INDEX_OP);
        descriptors.add(SCRIPT);
        descriptors.add(UPSERT_OPTION);

        propertyDescriptors = Collections.unmodifiableList(descriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> problems = new ArrayList<>(super.customValidate(validationContext));
        // Since Expression Language is allowed for index operation, we can't guarantee that we can catch
        // all invalid configurations, but we should catch them as soon as we can. For example, if the
        // Identifier Attribute property is empty, the Index Operation must evaluate to "index".
        String idAttribute = validationContext.getProperty(ID_ATTRIBUTE).getValue();
        String indexOp = validationContext.getProperty(INDEX_OP).getValue();

        if (StringUtils.isEmpty(idAttribute)) {
            switch (indexOp.toLowerCase()) {
                case "update":
                case "upsert":
                case "delete":
                case "":
                    problems.add(new ValidationResult.Builder()
                            .valid(false)
                            .subject(INDEX_OP.getDisplayName())
                            .explanation("If Identifier Attribute is not set, Index Operation must evaluate to \"index\"")
                            .build());
                    break;
                default:
                    break;
            }
        }

        // If "scripted_upsert" is selected as the upsert option, then there must be a script present.
        String upsertOption = validationContext.getProperty(UPSERT_OPTION).getValue();
        String script = validationContext.getProperty(SCRIPT).getValue();
        if (upsertOption != null && upsertOption.equals(SCRIPTED_UPSERT) && StringUtils.isBlank(script)) {
            problems.add(new ValidationResult.Builder()
                    .valid(false)
                    .subject(UPSERT_OPTION.getDisplayName())
                    .explanation(String.format("If %s is set to \"" + SCRIPTED_UPSERT + "\", a script must be provided.", UPSERT_OPTION.getDisplayName()))
                    .build());
        }

        return problems;
    }

    @OnScheduled
    public void setup(ProcessContext context) {
        super.setup(context);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final int batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger();

        final List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles.isEmpty()) {
            return;
        }

        final String id_attribute = context.getProperty(ID_ATTRIBUTE).getValue();

        // Authentication
        final String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        final String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();


        OkHttpClient okHttpClient = getClient();
        final ComponentLog logger = getLogger();

        // Keep track of the list of flow files that need to be transferred. As they are transferred, remove them from the list.
        List<FlowFile> flowFilesToTransfer = new LinkedList<>(flowFiles);

        final StringBuilder sb = new StringBuilder();
        final String baseUrl = trimToEmpty(context.getProperty(ES_URL).evaluateAttributeExpressions().getValue());
        HttpUrl.Builder urlBuilder = HttpUrl.parse(baseUrl).newBuilder().addPathSegment("_bulk");

        // Find the user-added properties and set them as query parameters on the URL
        for (Map.Entry<PropertyDescriptor, String> property : context.getProperties().entrySet()) {
            PropertyDescriptor pd = property.getKey();
            if (pd.isDynamic()) {
                if (property.getValue() != null) {
                    urlBuilder = urlBuilder.addQueryParameter(pd.getName(), context.getProperty(pd).evaluateAttributeExpressions().getValue());
                }
            }
        }
        final URL url = urlBuilder.build().url();

        for (FlowFile file : flowFiles) {
            final String index = context.getProperty(INDEX).evaluateAttributeExpressions(file).getValue();
            final Charset charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(file).getValue());
            if (StringUtils.isEmpty(index)) {
                logger.error("No value for index in for {}, transferring to failure", new Object[]{id_attribute, file});
                flowFilesToTransfer.remove(file);
                session.transfer(file, REL_FAILURE);
                continue;
            }
            final String docType = context.getProperty(TYPE).evaluateAttributeExpressions(file).getValue();
            String indexOp = context.getProperty(INDEX_OP).evaluateAttributeExpressions(file).getValue();
            if (StringUtils.isEmpty(indexOp)) {
                logger.error("No Index operation specified for {}, transferring to failure.", new Object[]{file});
                flowFilesToTransfer.remove(file);
                session.transfer(file, REL_FAILURE);
                continue;
            }

            String upsertOption = context.getProperty(UPSERT_OPTION).getValue();
            String script = context.getProperty(SCRIPT).evaluateAttributeExpressions(file).getValue();
            if (!StringUtils.isBlank(script)) {
                script = script.replace("\r\n", " ").replace('\n', ' ').replace('\r', ' ');
            }

            switch (indexOp.toLowerCase()) {
                case "index":
                case "update":
                case "upsert":
                case "delete":
                    break;
                default:
                    logger.error("Index operation {} not supported for {}, transferring to failure.", new Object[]{indexOp, file});
                    flowFilesToTransfer.remove(file);
                    session.transfer(file, REL_FAILURE);
                    continue;
            }

            final String id = (id_attribute != null) ? file.getAttribute(id_attribute) : null;

            // The ID must be valid for all operations except "index". For that case,
            // a missing ID indicates one is to be auto-generated by Elasticsearch
            if (id == null && !indexOp.equalsIgnoreCase("index")) {
                logger.error("Index operation {} requires a valid identifier value from a flow file attribute, transferring to failure.",
                        new Object[]{indexOp, file});
                flowFilesToTransfer.remove(file);
                session.transfer(file, REL_FAILURE);
                continue;
            }

            final StringBuilder json = new StringBuilder();
            session.read(file, in -> {
                json.append(IOUtils.toString(in, charset).replace("\r\n", " ").replace('\n', ' ').replace('\r', ' '));
            });

            if (indexOp.equalsIgnoreCase("index")) {
                sb.append("{\"index\": { \"_index\": \"");
                sb.append(index);
                sb.append("\", \"_type\": \"");
                sb.append(docType);
                sb.append("\"");
                if (!StringUtils.isEmpty(id)) {
                    sb.append(", \"_id\": \"");
                    sb.append(id);
                    sb.append("\"");
                }
                sb.append("}}\n");
                sb.append(json);
                sb.append("\n");
            } else if (indexOp.equalsIgnoreCase("upsert") || indexOp.equalsIgnoreCase("update")) {
                sb.append("{\"update\": { \"_index\": \"");
                sb.append(index);
                sb.append("\", \"_type\": \"");
                sb.append(docType);
                sb.append("\", \"_id\": \"");
                sb.append(id);
                sb.append("\" }\n");

                /*
                    Index Operation update/upsert both support upsert options "scripted_upsert" and "doc_as_upsert".
                    Whether the flowfile content is stored in "doc" or "upsert" depends
                    on the Index Operation value.
                 */
                if (upsertOption != null && upsertOption.equals(DOC_AS_UPSERT)) {
                    sb.append("{\"doc\": ");
                    sb.append(json);
                    sb.append(",\"" + DOC_AS_UPSERT + "\": true }");
                } else if (upsertOption != null && upsertOption.equals(SCRIPTED_UPSERT)) {
                    sb.append("{\"upsert\": {}");
                    sb.append(",\"" + SCRIPTED_UPSERT + "\": true");
                    sb.append(",\"script\": ");
                    sb.append(script);
                    sb.append("}");
                } else if (indexOp.equalsIgnoreCase("upsert")) {
                    sb.append("{\"upsert\": ");
                    sb.append(json + "}");
                } else {
                    sb.append("{\"doc\": ");
                    sb.append(json);
                    sb.append(" }");
                }
                sb.append("\n");
            } else if (indexOp.equalsIgnoreCase("delete")) {
                sb.append("{\"delete\": { \"_index\": \"");
                sb.append(index);
                sb.append("\", \"_type\": \"");
                sb.append(docType);
                sb.append("\", \"_id\": \"");
                sb.append(id);
                sb.append("\" }\n");
            }
        }

        if (!flowFilesToTransfer.isEmpty()) {
            RequestBody requestBody = RequestBody.create(MediaType.parse("application/json"), sb.toString());
            final Response getResponse;
            try {
                getResponse = sendRequestToElasticsearch(okHttpClient, url, username, password, "PUT", requestBody);
            } catch (final Exception e) {
                logger.error("Routing to {} due to exception: {}", new Object[]{REL_FAILURE.getName(), e}, e);
                flowFilesToTransfer.forEach((flowFileToTransfer) -> {
                    flowFileToTransfer = session.penalize(flowFileToTransfer);
                    session.transfer(flowFileToTransfer, REL_FAILURE);
                });
                flowFilesToTransfer.clear();
                return;
            }

            final int statusCode = getResponse.code();

            if (isSuccess(statusCode)) {
                ResponseBody responseBody = getResponse.body();
                try {
                    final byte[] bodyBytes = responseBody.bytes();

                    JsonNode responseJson = parseJsonResponse(new ByteArrayInputStream(bodyBytes));
                    boolean errors = responseJson.get("errors").asBoolean(false);
                    if (errors) {
                        ArrayNode itemNodeArray = (ArrayNode) responseJson.get("items");
                        if (itemNodeArray.size() > 0) {
                            // All items are returned whether they succeeded or failed, so iterate through the item array
                            // at the same time as the flow file list, moving each to success or failure accordingly
                            for (int i = itemNodeArray.size() - 1; i >= 0; i--) {
                                JsonNode itemNode = itemNodeArray.get(i);
                                FlowFile flowFile = flowFilesToTransfer.remove(i);
                                int status = itemNode.findPath("status").asInt();
                                if (!isSuccess(status)) {
                                    String reason = itemNode.findPath("//error/reason").asText();
                                    logger.error("Failed to insert {} into Elasticsearch due to {}, transferring to failure",
                                            new Object[]{flowFile, reason});
                                    session.transfer(flowFile, REL_FAILURE);

                                } else {
                                    session.transfer(flowFile, REL_SUCCESS);
                                    // Record provenance event
                                    session.getProvenanceReporter().send(flowFile, url.toString());
                                }
                            }
                        }
                    }
                    // Transfer any remaining flowfiles to success
                    flowFilesToTransfer.forEach(file -> {
                        session.transfer(file, REL_SUCCESS);
                        // Record provenance event
                        session.getProvenanceReporter().send(file, url.toString());
                    });
                } catch (IOException ioe) {
                    // Something went wrong when parsing the response, log the error and route to failure
                    logger.error("Error parsing Bulk API response: {}", new Object[]{ioe.getMessage()}, ioe);
                    session.transfer(flowFilesToTransfer, REL_FAILURE);
                    context.yield();
                }
            } else if (statusCode / 100 == 5) {
                // 5xx -> RETRY, but a server error might last a while, so yield
                logger.warn("Elasticsearch returned code {} with message {}, transferring flow file to retry. This is likely a server problem, yielding...",
                        new Object[]{statusCode, getResponse.message()});
                session.transfer(flowFilesToTransfer, REL_RETRY);
                context.yield();
            } else {  // 1xx, 3xx, 4xx, etc. -> NO RETRY
                logger.warn("Elasticsearch returned code {} with message {}, transferring flow file to failure", new Object[]{statusCode, getResponse.message()});
                session.transfer(flowFilesToTransfer, REL_FAILURE);
            }
            getResponse.close();
        }
    }
}
