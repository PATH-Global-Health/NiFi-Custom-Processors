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
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.stream.io.StreamUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.nio.charset.StandardCharsets;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;

import org.json.JSONArray;
import org.json.JSONObject;

import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_COUNT;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_ID;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_INDEX;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.SEGMENT_ORIGINAL_FILENAME;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.copyAttributesToOriginal;

@Tags({"sql", "alter", "database"})
@CapabilityDescription("Generate ALTER TABLE statement from JSON input to add a new column"
        + "or rename an existing column.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class Schema extends AbstractProcessor {

    public static final AllowableValue GENERIC = new AllowableValue("Generic", "Generic",
            "This denotes the Generic database.");

    public static final AllowableValue CLICKHOUSE = new AllowableValue("ClickHouse", "ClickHouse",
            "This denotes the ClickHouse database.");

    public static final AllowableValue POSTGRESQL = new AllowableValue("PostgreSQL", "PostgreSQL",
            "This denotes the PostgreSQL database.");

    public static final PropertyDescriptor DATABASE_TYPE = new PropertyDescriptor
            .Builder().name("DATABASE_TYPE")
            .displayName("Database Type")
            .description("Specify the database type for which we want to generate the ALTER TABLE SQL.")
            .defaultValue(GENERIC.getValue())
            .required(true)
            .allowableValues(GENERIC, CLICKHOUSE, POSTGRESQL)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();
    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor
            .Builder().name("TABLE_NAME")
            .displayName("Table Name")
            .description("Specify the name of the database table for which you want to generate the ALTER TABLE statement.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();
    public static final PropertyDescriptor COLUMN_NAME = new PropertyDescriptor
            .Builder().name("COLUMN_NAME")
            .displayName("Column Name")
            .description("Identify the column name in the JSON flowfile used as the key.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();
    public static final PropertyDescriptor COLUMN_TYPE = new PropertyDescriptor
            .Builder().name("COLUMN_TYPE")
            .displayName("Column Type")
            .description("Identify the column type in the JSON flowfile used as the key.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();
    public static final PropertyDescriptor REPLACE_COLUMN_NAME = new PropertyDescriptor
            .Builder().name("REPLACE_COLUMN_NAME")
            .displayName("Replace Column Name")
            .description("Specify the column name in the key of the JSON flowfile that will replace the old column.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();
    public static final PropertyDescriptor COLUMN_AFTER = new PropertyDescriptor
            .Builder().name("COLUMN_AFTER")
            .displayName("Column After")
            .description("Specify the name of the column after which you want to add the first new column.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();
    public static final PropertyDescriptor INSERT_UPDATE_KEY = new PropertyDescriptor
            .Builder().name("INSERT_UPDATE_KEY")
            .displayName("Insert or Update Key Name")
            .description("Specify the status of the key column in the JSON flowfile which determines"
                    + " whether ADD or UPDATE statements are generated in the ALTER TABLE.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile that was used to generate ALTER TABLE statements.")
            .build();
    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("FlowFiles will be routed to this relationship once the ALTER TABLE statements are generated.")
        .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("FlowFiles will be routed to this relationship if there is a failure.")
        .build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(DATABASE_TYPE);
        descriptors.add(TABLE_NAME);
        descriptors.add(COLUMN_NAME);
        descriptors.add(COLUMN_TYPE);
        descriptors.add(REPLACE_COLUMN_NAME);
        descriptors.add(COLUMN_AFTER);
        descriptors.add(INSERT_UPDATE_KEY);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(REL_ORIGINAL);
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
        // Read processor properties
        final String databaseType = context.getProperty(DATABASE_TYPE).evaluateAttributeExpressions().getValue();
        final String tableName = context.getProperty(TABLE_NAME).getValue();
        final String columnName = context.getProperty(COLUMN_NAME).getValue();
        final String columnType = context.getProperty(COLUMN_TYPE).getValue();
        final String replaceColumnName = context.getProperty(REPLACE_COLUMN_NAME).getValue();
        final String columnAfter = context.getProperty(COLUMN_AFTER).getValue();
        final String statusKey = context.getProperty(INSERT_UPDATE_KEY).getValue();
        int flowFileCount = 0;

        // Generate fragement UUID
        final String fragmentId = UUID.randomUUID().toString();

        // initialize the logger
        final ComponentLog logger = getLogger();

        // initialize the flow file from the current session.
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        //get flow file content
        final String jsonContent;
        try (final InputStream in = session.read(flowFile);
             final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            StreamUtils.copy(in, baos);
            jsonContent = new String(baos.toByteArray(), StandardCharsets.UTF_8);
        } catch (final Exception e) {
            throw new ProcessException(e);
        }

        try {
            // convert the flow file into json
            JSONArray jsonArray = new JSONArray(jsonContent);

            // Set common attributes like fragment ID and fragment count.
            Map<String, String> attributes = new HashMap<>();
            attributes.put(FRAGMENT_ID.key(), fragmentId);
            flowFileCount = jsonArray.length();
            attributes.put(FRAGMENT_COUNT.key(), Integer.toString(flowFileCount));

            String prevColumnName = "";
            // Iterate through the JSONArray
            for (int i = 0; i < jsonArray.length(); i++) {
                // Get a JSONObject from the JSONArray
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                

                // Get the properties from the JSONObject
                String cName = jsonObject.getString(columnName).replaceAll("\\s+","");

                String cType = jsonObject.getString(columnType);
                String status = jsonObject.getString(statusKey);
                String replaceName = jsonObject.getString(replaceColumnName);

                FlowFile sqlFlowFile = session.create();

                // prepare the sql statement
                String sql = "";
                if (status.equals( "insert")) {
                    if(i == 0) {
                        sql = String.format("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s AFTER %s;", tableName, cName, cType, columnAfter);
                    } else {
                        sql = String.format("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s AFTER %s;", tableName, cName, cType, prevColumnName);
                    }
                    prevColumnName = cName;
                } else {
                    sql = String.format("ALTER TABLE %s RENAME COLUMN IF EXISTS %s to %s;", tableName, cName, replaceName);
                }

                // write the sql statement in the flow file
                final String finalSql = sql;
                sqlFlowFile = session.write(sqlFlowFile, outputStream -> outputStream.write(finalSql.getBytes()));

                // set the segment original file name as attribute
                attributes.put(SEGMENT_ORIGINAL_FILENAME.key(), sqlFlowFile.getAttribute(CoreAttributes.FILENAME.key()));
                // set the fragement index
                attributes.put(FRAGMENT_INDEX.key(), Integer.toString(i+1));

                // transfer the SQL flow file to the success relationship
                session.transfer(session.putAllAttributes(sqlFlowFile, attributes), REL_SUCCESS);
            }
        } catch (ProcessException ex) {
            logger.error("Unable to generate the schema", ex);
            session.transfer(flowFile, REL_FAILURE);
        }
        
        // copy fragmentId and Fragment count to original flow file
        flowFile = copyAttributesToOriginal(session, flowFile, fragmentId, flowFileCount);
        // transfer the original json file to the original relationship
        session.transfer(flowFile, REL_ORIGINAL);
        logger.info("Schema {} into {} FlowFiles", new Object[]{flowFile, flowFileCount});
    }
}
