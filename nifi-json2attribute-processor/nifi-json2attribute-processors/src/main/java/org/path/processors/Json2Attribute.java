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
package org.path.processors;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.logging.ComponentLog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.nio.charset.StandardCharsets;

import com.google.gson.*;

@Tags({"json", "attribute"})
@CapabilityDescription("Converts a JSON representation of the input FlowFile into Attributes.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class Json2Attribute extends AbstractProcessor {

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("FlowFiles will be routed to this relationship once the JSON content is converted to attribute.")
        .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("FlowFiles will be routed to this relationship if there is a failure.")
        .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        // initialize the logger
        final ComponentLog logger = getLogger();

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        
        //get flow file content
        final String jsonString;
        try (final InputStream in = session.read(flowFile);
             final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            StreamUtils.copy(in, baos);
            jsonString = new String(baos.toByteArray(), StandardCharsets.UTF_8);
        } catch (final Exception e) {
            throw new ProcessException(e);
        }

        try {
            JsonParser parser = new JsonParser();
            // convert the flow file into json
            JsonObject jsonObject = parser.parse(jsonString).getAsJsonObject();
            
            //define hashmap to store attributes
            Map<String, String> attributes = new HashMap<>();

            for (String key : jsonObject.keySet()) {
                JsonElement jsonValue = jsonObject.get(key);
                // String stringValue = new Gson().toJson(jsonValue);
                String stringValue = jsonValue.getAsString().replaceAll("\\s+", "");
                attributes.put(key, stringValue);
            }

            session.transfer(session.putAllAttributes(flowFile, attributes), REL_SUCCESS);
        
         } catch (ProcessException ex) {
            logger.error("Unable to convert JSON to attribute", ex);
            session.transfer(flowFile, REL_FAILURE);
        }

    }
}
