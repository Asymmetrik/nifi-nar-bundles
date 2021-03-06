/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asymmetrik.nifi.processors;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.DynamicRelationship;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.Tuple;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
@SupportsBatching
@Tags({"route", "bit", "mask"})
@DynamicProperty(name = "Bit mask", value = "The value to set it to",
        description = "Specifies dynamic relationship with provided bit masks")
@DynamicRelationship(name = "Name from Dynamic Property", description = "FlowFiles that match the Dynamic Property value's bit mask")
@CapabilityDescription("Routes based on bit matches")
public class RouteOnBitMask extends AbstractProcessor {

    static final PropertyDescriptor ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("Target attribute name")
            .displayName("Target attribute name")
            .description("The attribute name evaluating to long value to apply the bit mask")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor FLIP_BIT = new PropertyDescriptor.Builder()
            .name("Flip bit on match")
            .displayName("Flip bit on match")
            .description("Flip the matching bit to zero on match")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    static final Relationship UNMATCHED = new Relationship.Builder()
            .name("unmatched")
            .description("Files are transferred here when no matches are found")
            .build();

    static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files are transferred here when an error occurs")
            .build();

    // dynamic relationships and their corresponding bit mask values
    private final ConcurrentMap<String, MutablePair<Relationship, Long>> routeMasks = new ConcurrentHashMap<>();

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) {
        FlowFile original = session.get();
        if (original == null) {
            return;
        }

        // name of attribute to mask against
        String name = context.getProperty(ATTRIBUTE_NAME).evaluateAttributeExpressions(original).getValue();
        Map<String, String> attributes = new HashMap<>(original.getAttributes());

        final String bitValue = attributes.get(name);
        if (StringUtils.isEmpty(bitValue)) {
            session.transfer(original, FAILURE);
            return;
        }

        // all relationships who's masks matched specified attribute value
        Set<Relationship> matched = new HashSet<>();
        // the bitwise OR of all masks matched
        long matchedMasks = 0;

        try {
            // evaluated attribute value to test mask against
            final long value = Long.valueOf(bitValue);

            for (MutablePair<Relationship, Long> entry : routeMasks.values()) {
                // the mask associated with this relationship
                final long mask = entry.getValue();

                // test for match
                if ((value & mask) == mask) {
                    matched.add(entry.getKey());
                    matchedMasks |= mask;
                }
            }

            if (!matched.isEmpty()) {
                if (context.getProperty(FLIP_BIT).asBoolean()) {
                    attributes.put(name, String.valueOf(value ^ matchedMasks));
                }

                for (Relationship relationship : matched) {
                    FlowFile cloned = session.clone(original);
                    cloned = session.putAllAttributes(cloned, attributes);
                    session.transfer(cloned, relationship);
                }

                session.remove(original);
            } else {
                session.transfer(original, UNMATCHED);
            }

        } catch (Exception e) {
            // nothing should have been transferred on error
            getLogger().error(e.getMessage(), e);
            session.transfer(original, FAILURE);
        }
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (!descriptor.isDynamic()) {
            return;
        }

        MutablePair<Relationship, Long> existing = routeMasks.remove(descriptor.getName());

        if (newValue != null && oldValue == null) {
            // a new dynamic property was added
            routeMasks.put(descriptor.getName(),
                    MutablePair.of(
                            new Relationship.Builder().name(descriptor.getName()).build(),
                            Long.valueOf(newValue)
                    )
            );
        } else if (newValue != null && existing != null) {
            // an existing dynamic property's value changed

            existing.setRight(Long.valueOf(newValue));
            routeMasks.put(descriptor.getName(), existing);
        }
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(ATTRIBUTE_NAME, FLIP_BIT);
    }

    @Override
    public Set<Relationship> getRelationships() {
        Set<Relationship> relationships = routeMasks.values().stream(). map(MutablePair::getLeft).collect(Collectors.toSet());
        relationships.add(UNMATCHED);
        relationships.add(FAILURE);
        return relationships;
    }
}
