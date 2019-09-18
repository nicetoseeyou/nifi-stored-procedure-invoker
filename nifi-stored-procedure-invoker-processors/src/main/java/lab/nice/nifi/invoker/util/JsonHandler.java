package lab.nice.nifi.invoker.util;

import com.fasterxml.jackson.core.JsonGenerator;
import lab.nice.nifi.invoker.common.AttributeConstant;
import lab.nice.nifi.invoker.common.ProcedureParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.CharBuffer;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;

public final class JsonHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonHandler.class);

    private static final String RESULT_SET_HEADER = "RESULTS";
    private static final String OUTPUT_HEADER = "OUTPUTS";
    private static final String OUTPUT_PREFIX = "output_";
    private static final int BUFFER_SIZE = 256;

    private JsonHandler() {
    }

    public static void retrieveCallableStatement(final CallableStatement statement, final JsonGenerator jsonGenerator,
                                                 final List<ProcedureParameter> procedureParameters) {

    }

    public static void retrieveOutputs(final CallableStatement statement, final JsonGenerator jsonGenerator,
                                       final List<ProcedureParameter> procedureParameters) throws IOException, SQLException {
        if (null == procedureParameters && !procedureParameters.isEmpty()) {
            boolean isEmpty = true;
            for (ProcedureParameter parameter : procedureParameters) {
                if (AttributeConstant.PROCEDURE_TYPE_OUT.equals(parameter.getParameterType())
                        || AttributeConstant.PROCEDURE_TYPE_INOUT.equals(parameter.getParameterType())) {
                    if (isEmpty) {
                        jsonGenerator.writeObjectFieldStart(OUTPUT_HEADER);
                        isEmpty = false;
                    }
                    String fieldName = parameter.getParameterName();
                    if (Types.CLOB == parameter.getJdbcType().getVendorTypeNumber()) {
                        writeJson(jsonGenerator, fieldName, statement.getCharacterStream(parameter.getParameterIndex()));
                    }else if (Types.NCLOB == parameter.getJdbcType().getVendorTypeNumber()){
                        writeJson(jsonGenerator, fieldName, statement.getNCharacterStream(parameter.getParameterIndex()));
                    }
                }
            }
            if (!isEmpty) {
                jsonGenerator.writeEndObject();
            }
        }
    }

    /**
     * Streaming retrieve ResultSet and update count of Statement into JSON.
     *
     * @param statement     the statement to retrieve
     * @param jsonGenerator the JSON writer
     * @throws SQLException if failed to retrieve statement
     * @throws IOException  if failed to read CLOB/NCLOB value in ResultSet or failed to write JSON content
     */
    public static void retrieveResults(final Statement statement, final JsonGenerator jsonGenerator)
            throws SQLException, IOException {
        boolean hasResults = hasMoreResults(statement);
        if (hasResults) {
            jsonGenerator.writeArrayFieldStart(RESULT_SET_HEADER);
            while (hasResults) {
                if (statement.getUpdateCount() != -1) {
                    jsonGenerator.writeObject(statement.getUpdateCount());
                } else {
                    retrieveResultSet(statement.getResultSet(), jsonGenerator);
                }
                hasResults = hasMoreResults(statement);
            }
            jsonGenerator.writeEndArray();
        }
    }

    /**
     * Streaming retrieve ResultSet and write into JSON, empty ResultSet will be skipped.
     *
     * @param resultSet     the ResultSet to retrieve
     * @param jsonGenerator the JSON writer
     * @throws SQLException if failed to retrieve ResultSet
     * @throws IOException  if failed to read CLOB/NCLOB value in ResultSet or failed to write JSON content
     */
    public static void retrieveResultSet(final ResultSet resultSet, final JsonGenerator jsonGenerator)
            throws SQLException, IOException {
        if (resultSet.isBeforeFirst()) {
            final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            final int columnCount = resultSetMetaData.getColumnCount();
            jsonGenerator.writeStartArray();
            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    if (resultSetMetaData.getColumnType(i) == Types.CLOB) {
                        writeJson(jsonGenerator, resultSetMetaData.getColumnName(i), resultSet.getCharacterStream(i));
                    } else if (resultSetMetaData.getColumnType(i) == Types.NCLOB) {
                        writeJson(jsonGenerator, resultSetMetaData.getColumnName(i), resultSet.getNCharacterStream(i));
                    } else {
                        writeJson(jsonGenerator, resultSetMetaData.getColumnName(i), resultSet.getObject(i));
                    }
                }
            }
            jsonGenerator.writeEndArray();
        } else {
            //empty ResultSet
            LOGGER.warn("Empty ResultSet, will skip it");
        }
    }

    /**
     * Write JSON from character stream. JSON NULL will be written for NULL stream.
     *
     * @param jsonGenerator the JSON writer
     * @param fieldName     the field name of the JSON value
     * @param reader        the character stream
     * @throws IOException if failed to read character stream or failed to write JSON content
     */
    public static void writeJson(final JsonGenerator jsonGenerator, final String fieldName, final Reader reader)
            throws IOException {
        if (null == reader) {
            jsonGenerator.writeNullField(fieldName);
        } else {
            final StringBuffer stringBuffer = new StringBuffer();
            final CharBuffer charBuffer = CharBuffer.allocate(BUFFER_SIZE);
            while (reader.read(charBuffer) != -1) {
                charBuffer.flip();
                stringBuffer.append(charBuffer.toString());
                charBuffer.clear();
            }
            jsonGenerator.writeStringField(fieldName, stringBuffer.toString());
        }
    }

    /**
     * Write Java Object to JSON, JSON NULL will be written for NULL Java object.
     *
     * @param jsonGenerator the JSON writer
     * @param fieldName     the field name of the JSON value
     * @param value         the Json value
     * @throws IOException if failed to write JSON content or unsupported Java type
     */
    public static void writeJson(final JsonGenerator jsonGenerator, final String fieldName, final Object value)
            throws IOException {
        if (null == value) {
            jsonGenerator.writeNullField(fieldName);
        } else {
            jsonGenerator.writeObjectField(fieldName, value);
        }
    }

    private static boolean hasMoreResults(final Statement statement) {
        try {
            return statement.getMoreResults() || statement.getUpdateCount() != -1;
        } catch (SQLException e) {
            LOGGER.warn("No more ResultSet, will return false.", e);
            return false;
        }
    }
}
