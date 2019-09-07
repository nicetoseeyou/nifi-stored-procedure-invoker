package lab.nice.nifi.invoker;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import lab.nice.nifi.invoker.util.JdbcUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import java.io.UnsupportedEncodingException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@Tags({"procedure", "execute", "rdbms", "database"})
@CapabilityDescription("Execute stored procedure via JDBC.")
@ReadsAttributes({
        @ReadsAttribute(attribute = "procedure.args.in.N.type",
                description = "IN argument type for parametrized stored procedure statement. The type of each Parameter "
                        + "is specified as an integer that represents the JDBC Type of the parameter."),
        @ReadsAttribute(attribute = "procedure.args.in.N.value",
                description = "IN argument value for parametrized stored procedure statement. The value of the Parameters "
                        + "are specified as procedure.args.in.1.value, procedure.args.in.2.value and so on. The type of the "
                        + "procedure.args.in.1.value Parameter is specified by the procedure.args.in.1.type attribute."),
        @ReadsAttribute(attribute = "procedure.args.in.N.format",
                description = "This attribute is always optional, but default options may not always work for your data. "
                        + "Incoming FlowFile attribute are expected to contain parametrized stored procedure statement. "
                        + "In some cases a format option needs to be specified, currently this is only applicable for "
                        + "binary data types, dates, times and timestamps. Binary Data Types (defaults to 'ascii') - "
                        + "ascii: each string character in your attribute value represents a single byte. "
                        + "This is the format provided by Avro Processors. "
                        + "base64: the string is a Base64 encoded string that can be decoded to bytes. "
                        + "hex: the string is hex encoded with all letters in upper case and no '0x' at the beginning. "
                        + "Dates/Times/Timestamps - "
                        + "Date, Time and Timestamp formats all support both custom formats or named "
                        + "format ('yyyy-MM-dd','ISO_OFFSET_DATE_TIME') as specified according to java.time.format.DateTimeFormatter. "
                        + "If not specified, a long value input is expected to be an unix epoch (milli seconds from 1970/1/1), "
                        + "or a string value in 'yyyy-MM-dd' format for Date, "
                        + "'HH:mm:ss.SSS' for Time (some database engines e.g. Derby or MySQL do not support milliseconds "
                        + "and will truncate milliseconds), 'yyyy-MM-dd HH:mm:ss.SSS' for Timestamp is used."),

        @ReadsAttribute(attribute = "procedure.args.out.N.type",
                description = "OUT argument type for parametrized stored procedure statement. The type of each Parameter "
                        + "is specified as an integer that represents the JDBC Type of the parameter."),
        @ReadsAttribute(attribute = "procedure.args.out.N.name",
                description = "OUT argument name for parametrized stored procedure statement. The value of the Parameters "
                        + "are specified as procedure.args.out.1.name, procedure.args.out.2.name and so on. "
                        + "The return value of the procedure.args.out.1.type Parameter is named by the "
                        + "procedure.args.out.1.name attribute. This attribute would be used to named "
                        + "the stored procedure output value."),

        @ReadsAttribute(attribute = "procedure.args.inout.N.type",
                description = "INOUT argument type for parametrized stored procedure statement. The type of each Parameter "
                        + "is specified as an integer that represents the JDBC Type of the parameter."),
        @ReadsAttribute(attribute = "procedure.args.inout.N.value",
                description = "INOUT argument value for parametrized stored procedure statement. The value of the Parameters "
                        + "are specified as procedure.args.inout.1.value, procedure.args.inout.2.value and so on. The type of the "
                        + "procedure.args.inout.1.value Parameter is specified by the procedure.args.inout.1.type attribute."),
        @ReadsAttribute(attribute = "procedure.args.inout.N.name",
                description = "INOUT argument name for parametrized stored procedure statement. The value of the Parameters "
                        + "are specified as procedure.args.inout.1.name, procedure.args.inout.2.name and so on. "
                        + "The return value of the procedure.args.inout.1.type Parameter is named by the "
                        + "procedure.args.inout.1.name attribute. This attribute would be used to named "
                        + "the stored procedure output value."),
        @ReadsAttribute(attribute = "procedure.args.inout.N.format",
                description = "This attribute is always optional, but default options may not always work for your data. "
                        + "Incoming FlowFile attribute are expected to contain parametrized stored procedure statement. "
                        + "In some cases a format option needs to be specified, currently this is only applicable for "
                        + "binary data types, dates, times and timestamps. Binary Data Types (defaults to 'ascii') - "
                        + "ascii: each string character in your attribute value represents a single byte. "
                        + "This is the format provided by Avro Processors. "
                        + "base64: the string is a Base64 encoded string that can be decoded to bytes. "
                        + "hex: the string is hex encoded with all letters in upper case and no '0x' at the beginning. "
                        + "Dates/Times/Timestamps - "
                        + "Date, Time and Timestamp formats all support both custom formats or named "
                        + "format ('yyyy-MM-dd','ISO_OFFSET_DATE_TIME') as specified according to java.time.format.DateTimeFormatter. "
                        + "If not specified, a long value input is expected to be an unix epoch (milli seconds from 1970/1/1), "
                        + "or a string value in 'yyyy-MM-dd' format for Date, "
                        + "'HH:mm:ss.SSS' for Time (some database engines e.g. Derby or MySQL do not support milliseconds "
                        + "and will truncate milliseconds), 'yyyy-MM-dd HH:mm:ss.SSS' for Timestamp is used.")
})
@WritesAttributes({
        @WritesAttribute(
                attribute = "procedure.execute.duration",
                description = "Duration of the stored procedure execution in milliseconds"),
        @WritesAttribute(
                attribute = "procedure.return.resultset.count",
                description = "The total of the ResultSet returned by stored procedure"
        ),
        @WritesAttribute(
                attribute = "procedure.return.row.count",
                description = "The total row of all the ResultSets returned by stored procedure"
        ),
        @WritesAttribute(
                attribute = "procedure.return.output.count",
                description = "The total of the output returned by the stored procedure"
        )
})
@DynamicProperty(name = "The name of a stored procedure parameter configuration property",
        value = "The value of a stored procedure parameter configuration property",
        description = "Properties for parametrized stored procedure arguments (procedure.args.in.N.type, "
                + "procedure.args.in.N.value, procedure.args.in.N.format, procedure.args.out.N.type, "
                + "procedure.args.inout.N.type, procedure.args.inout.N.value, procedure.args.inout.N.format). "
                + "In the event a dynamic property represents a property that was already set, "
                + "its value will be override by the incoming FlowFile attribute.")
public class ExecuteStoredProcedure extends AbstractProcessor {
    private static final String PROCEDURE_EXECUTE_DURATION = "procedure.execute.duration";
    private static final String PROCEDURE_RETURN_RESULTSET_COUNT = "procedure.return.resultset.count";
    private static final String PROCEDURE_RETURN_ROW_COUNT = "procedure.return.row.count";
    private static final String PROCEDURE_RETURN_OUTPUT_COUNT = "procedure.return.output.count";
    private static final String STORED_PROCEDURE_STATEMENT_KEY = "stored.procedure.statement";
    private static final Pattern DYNAMIC_ATTRIBUTE_PATTERN = Pattern.compile("procedure\\.args\\.(in|out|inout)\\.(\\d+)\\.type");
    private static final Pattern NUMBER_PATTERN = Pattern.compile("-?\\d+");
    private static final Pattern LONG_PATTERN = Pattern.compile("^-?\\d{1,19}$");

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from stored procedure execution return result.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Stored procedure execution failed. Incoming FlowFile will be penalized and routed to this relationship")
            .build();

    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("dbcp.service")
            .displayName("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor STORED_PROCEDURE_STATEMENT = new PropertyDescriptor.Builder()
            .name("stored.procedure.statement")
            .displayName("Stored Procedure Statement")
            .description("The stored procedure statement to execute. The statement can be empty, a constant value, "
                    + "or built from attributes using Expression Language. If this property is specified, it will be "
                    + "used regardless of the content of incoming FlowFile. If this property is empty, the attributes of "
                    + "the incoming FlowFile is expected to contain an attribute 'stored.procedure.statement' with a valid "
                    + "stored procedure statement. Note that Expression Language is not evaluated for FlowFile contents.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor PROCEDURE_EXECUTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("procedure.max.wait.time")
            .displayName("Max Wait Time in Seconds")
            .description("The maximum amount of time allowed for a running stored procedure statement, "
                    + "zero means there is no limit. Max time less than 1 second will be equal to zero.")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .sensitive(false)
            .build();

    private final Set<Relationship> relationships;
    private final List<PropertyDescriptor> propertyDescriptors;

    public ExecuteStoredProcedure() {
        final Set<Relationship> r = new HashSet<>();
        r.add(REL_SUCCESS);
        r.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(r);

        final List<PropertyDescriptor> p = new ArrayList<>();
        p.add(DBCP_SERVICE);
        p.add(STORED_PROCEDURE_STATEMENT);
        p.add(PROCEDURE_EXECUTION_TIMEOUT);
        propertyDescriptors = Collections.unmodifiableList(p);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(true)
                .dynamic(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();
    }

    @OnScheduled
    public void setup(final ProcessContext processContext) {
        // If the stored procedure statement is not set, then an incoming FlowFile is needed.
        // Otherwise fail the initialization
        if (!processContext.getProperty(STORED_PROCEDURE_STATEMENT).isSet() && !processContext.hasIncomingConnection()) {
            final String errorString = "Either the stored procedure statement must be specified or there must be"
                    + " an incoming connection providing FlowFile(s) containing a stored procedure statement";
            getLogger().error(errorString);
            throw new ProcessException(errorString);
        }
    }

    @Override
    public void onTrigger(final ProcessContext processContext, final ProcessSession processSession) throws ProcessException {
        FlowFile flowFile = null;
        if (processContext.hasIncomingConnection()) {
            flowFile = processSession.get();
            if (flowFile == null && processContext.hasNonLoopConnection()) {
                return;
            }
        }
        final DBCPService dbcpService = processContext.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        final int timeout = processContext.getProperty(PROCEDURE_EXECUTION_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();
        final StopWatch stopWatch = new StopWatch(true);
        final String procedure;
        if (processContext.getProperty(STORED_PROCEDURE_STATEMENT).isSet()) {
            procedure = processContext.getProperty(STORED_PROCEDURE_STATEMENT).evaluateAttributeExpressions(flowFile).getValue();
        } else {
            if (flowFile != null) {
                procedure = flowFile.getAttribute(STORED_PROCEDURE_STATEMENT_KEY);
            } else {
                throw new ProcessException("Stored procedure statement must be specified.");
            }
        }
        if (StringUtils.isBlank(procedure)) {
            throw new ProcessException("Stored Procedure Statement could not be empty.");
        }
        final Map<Integer, OutputMeta> outParameters = new HashMap<>();
        final ResultMeta resultMeta = new ResultMeta();
        try (final Connection connection = dbcpService.getConnection();
             final CallableStatement callableStatement = connection.prepareCall(procedure)) {
            callableStatement.setQueryTimeout(timeout);
            final Map<String, String> attributes = processContext.getAllProperties();
            inputParameter(callableStatement, attributes, outParameters);
            if (flowFile != null) {
                inputParameter(callableStatement, flowFile.getAttributes(), outParameters);
            }
            boolean results = callableStatement.execute();
            FlowFile resultSetFF;
            if (results || !outParameters.isEmpty()) {
                if (flowFile == null) {
                    resultSetFF = processSession.create();
                } else {
                    resultSetFF = processSession.create(flowFile);
                    resultSetFF = processSession.putAllAttributes(resultSetFF, flowFile.getAttributes());
                }
                resultSetFF = processSession.write(resultSetFF, outputStream -> {
                    final ObjectMapper objectMapper = new ObjectMapper();
                    try (final JsonGenerator jsonGenerator = objectMapper.getFactory()
                            .createGenerator(outputStream, JsonEncoding.UTF8)) {
                        boolean hasResultSet = results;
                        jsonGenerator.writeStartObject(); // start of root json
                        if (hasResultSet) {
                            jsonGenerator.writeArrayFieldStart("ResultSets"); //start of ResultSets
                            while (hasResultSet) {
                                final ResultSet resultSet = callableStatement.getResultSet();
                                final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                                final int columnCount = resultSetMetaData.getColumnCount();
                                jsonGenerator.writeStartArray(); //start of one ResultSet
                                while (resultSet.next()) {
                                    jsonGenerator.writeStartObject();//start of row
                                    for (int i = 0; i <= columnCount; i++) {
                                        jsonGenerator.writeObjectField(resultSetMetaData.getColumnName(i), resultSet.getObject(i));
                                    }
                                    jsonGenerator.writeEndObject();//end of row
                                    resultMeta.incRowCount();
                                }
                                jsonGenerator.writeEndArray();//end of one ResultSet
                                resultMeta.incResultSetCount();
                                try {
                                    hasResultSet = callableStatement.getMoreResults();
                                } catch (SQLException e) {
                                    getLogger().warn("No more ResultSet.", e);
                                    hasResultSet = false;
                                }
                            }
                            jsonGenerator.writeEndArray(); //end of ResultSets
                        }
                        if (!outParameters.isEmpty()) {
                            jsonGenerator.writeObjectFieldStart("Output"); //start of output
                            for (Map.Entry<Integer, OutputMeta> ome : outParameters.entrySet()) {
                                final Integer index = ome.getKey();
                                final OutputMeta meta = ome.getValue();
                                //find a better way to evaluate output with exact jdbc type
                                jsonGenerator.writeObjectField(meta.name, callableStatement.getObject(index));
                                resultMeta.incOutputCount();
                            }
                            jsonGenerator.writeEndObject();//end of output
                        }
                        jsonGenerator.writeEndObject();//end of root json
                        jsonGenerator.flush();
                    } catch (SQLException e) {
                        throw new ProcessException(e);
                    }
                });
                long duration = stopWatch.getElapsed(TimeUnit.MILLISECONDS);
                resultSetFF = processSession.putAttribute(resultSetFF, PROCEDURE_EXECUTE_DURATION,
                        String.valueOf(duration));
                resultSetFF = processSession.putAttribute(resultSetFF, PROCEDURE_RETURN_RESULTSET_COUNT,
                        String.valueOf(resultMeta.getResultSetCount()));
                resultSetFF = processSession.putAttribute(resultSetFF, PROCEDURE_RETURN_ROW_COUNT,
                        String.valueOf(resultMeta.getRowCount()));
                resultSetFF = processSession.putAttribute(resultSetFF, PROCEDURE_RETURN_OUTPUT_COUNT,
                        String.valueOf(resultMeta.getOutputCount()));
                processSession.getProvenanceReporter().modifyContent(resultSetFF, "Procedure executed. " + resultMeta, duration);
                processSession.transfer(resultSetFF, REL_SUCCESS);
            } else {
                //If we had at least one result then it's OK to drop the original file, but if we had no results then
                //  pass the original flow file down the line to trigger downstream processors
                long duration = stopWatch.getElapsed(TimeUnit.MILLISECONDS);
                if (flowFile != null) {
                    if (resultMeta.getResultSetCount() > 0 || resultMeta.getOutputCount() > 0) {
                        processSession.remove(flowFile);
                    } else {
                        flowFile = processSession.putAttribute(flowFile, PROCEDURE_EXECUTE_DURATION,
                                String.valueOf(duration));
                        flowFile = processSession.putAttribute(flowFile, PROCEDURE_RETURN_RESULTSET_COUNT,
                                String.valueOf(resultMeta.getResultSetCount()));
                        flowFile = processSession.putAttribute(flowFile, PROCEDURE_RETURN_ROW_COUNT,
                                String.valueOf(resultMeta.getRowCount()));
                        flowFile = processSession.putAttribute(flowFile, PROCEDURE_RETURN_OUTPUT_COUNT,
                                String.valueOf(resultMeta.getOutputCount()));
                        flowFile = processSession.write(flowFile, outputStream -> {
                            //do nothing
                        });
                        processSession.getProvenanceReporter().modifyContent(flowFile, "Procedure executed." + resultMeta, duration);
                        processSession.transfer(flowFile, REL_SUCCESS);
                    }
                } else {
                    flowFile = processSession.create();
                    flowFile = processSession.putAttribute(flowFile, PROCEDURE_EXECUTE_DURATION,
                            String.valueOf(duration));
                    flowFile = processSession.putAttribute(flowFile, PROCEDURE_RETURN_RESULTSET_COUNT,
                            String.valueOf(resultMeta.getResultSetCount()));
                    flowFile = processSession.putAttribute(flowFile, PROCEDURE_RETURN_ROW_COUNT,
                            String.valueOf(resultMeta.getRowCount()));
                    flowFile = processSession.putAttribute(flowFile, PROCEDURE_RETURN_OUTPUT_COUNT,
                            String.valueOf(resultMeta.getOutputCount()));
                    flowFile = processSession.write(flowFile, outputStream -> {
                        //do nothing
                    });
                    processSession.getProvenanceReporter().modifyContent(flowFile, "Procedure executed." + resultMeta, duration);
                    processSession.transfer(flowFile, REL_SUCCESS);
                }
            }
        } catch (final ProcessException | SQLException e) {
            //If we had at least one result then it's OK to drop the original file, but if we had no results then
            //  pass the original flow file down the line to trigger downstream processors
            if (flowFile == null) {
                // This can happen if any exceptions occur while setting up the connection, statement, etc.
                getLogger().error("Unable to execute stored procedure {} due to {}. No FlowFile to route to failure",
                        new Object[]{procedure, e});
                processContext.yield();
            } else {
                if (processContext.hasIncomingConnection()) {
                    getLogger().error("Unable to execute stored procedure {} for {} due to {}; routing to failure",
                            new Object[]{procedure, flowFile, e});
                    flowFile = processSession.penalize(flowFile);
                } else {
                    getLogger().error("Unable to execute SQL select query {} due to {}; routing to failure",
                            new Object[]{procedure, e});
                    processContext.yield();
                }
                processSession.transfer(flowFile, REL_FAILURE);
            }
        }
    }

    private void inputParameter(final CallableStatement callableStatement, final Map<String, String> attributes,
                                final Map<Integer, OutputMeta> outParameters) throws SQLException {
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            final String key = entry.getKey();
            final Matcher dynamicMatcher = DYNAMIC_ATTRIBUTE_PATTERN.matcher(key);
            if (dynamicMatcher.matches()) {
                final boolean isNumeric = NUMBER_PATTERN.matcher(entry.getValue()).matches();
                if (!isNumeric) {
                    throw new SQLDataException("Value of the " + key + " attribute is '" + entry.getValue()
                            + "', which is not a valid JDBC numeral type");
                }
                final String parameterType = dynamicMatcher.group(1);
                final Integer parameterIndex = Integer.parseInt(dynamicMatcher.group(2));
                final int jdbcType = Integer.parseInt(entry.getValue());
                if (parameterType.equals("in") || parameterType.equals("inout")) {
                    final String valueAttributeName = String.format("procedure.args.%d.%s.value", parameterIndex, parameterType);
                    final String parameterValue = attributes.get(valueAttributeName);
                    final String formatAttributeName = String.format("procedure.args.%d.%s.format", parameterIndex, parameterType);
                    final String parameterFormat = attributes.getOrDefault(formatAttributeName, "");
                    try {
                        JdbcUtils.setParameter(callableStatement, valueAttributeName, parameterIndex, parameterValue, jdbcType, parameterFormat);
                    } catch (final NumberFormatException e) {
                        throw new SQLDataException("The value of the " + valueAttributeName + " is '" + parameterValue + "', which cannot be converted into the necessary data type", e);
                    } catch (ParseException e) {
                        throw new SQLDataException("The value of the " + valueAttributeName + " is '" + parameterValue + "', which cannot be converted to a timestamp", e);
                    } catch (UnsupportedEncodingException e) {
                        throw new SQLDataException("The value of the " + valueAttributeName + " is '" + parameterValue + "', which cannot be converted to UTF-8", e);
                    }
                }
                if (parameterType.equals("out") || parameterType.equals("inout")) {
                    final String nameAttribute = String.format("procedure.args.%d.%s.name", parameterIndex, parameterType);
                    final String name = attributes.getOrDefault(nameAttribute, parameterType + "-" + parameterIndex);
                    final OutputMeta outputMeta = new OutputMeta(name, jdbcType);
                    outParameters.put(parameterIndex, outputMeta);
                    callableStatement.registerOutParameter(parameterIndex, jdbcType);
                }
            }
        }
    }

    static class OutputMeta {
        final String name;
        final Integer jdbcType;

        OutputMeta(final String name, final Integer jdbcType) {
            this.name = name;
            this.jdbcType = jdbcType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OutputMeta that = (OutputMeta) o;
            return Objects.equals(name, that.name) &&
                    Objects.equals(jdbcType, that.jdbcType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, jdbcType);
        }

        @Override
        public String toString() {
            return "OutputMeta{" +
                    "name='" + name + '\'' +
                    ", jdbcType=" + jdbcType +
                    '}';
        }
    }

    static class ResultMeta {
        private int resultSetCount = 0;
        private int rowCount = 0;
        private int outputCount = 0;

        void incResultSetCount() {
            this.resultSetCount++;
        }

        void incRowCount() {
            this.rowCount++;
        }

        void incOutputCount() {
            this.outputCount++;
        }

        int getResultSetCount() {
            return resultSetCount;
        }

        int getRowCount() {
            return rowCount;
        }

        int getOutputCount() {
            return outputCount;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ResultMeta that = (ResultMeta) o;
            return resultSetCount == that.resultSetCount &&
                    rowCount == that.rowCount &&
                    outputCount == that.outputCount;
        }

        @Override
        public int hashCode() {
            return Objects.hash(resultSetCount, rowCount, outputCount);
        }

        @Override
        public String toString() {
            return "Stored procedure return {" +
                    "ResultSetCount=" + resultSetCount +
                    ", RowCount=" + rowCount +
                    ", OutputCount=" + outputCount +
                    '}';
        }
    }
}
