package lab.nice.nifi.invoker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lab.nice.nifi.invoker.util.JdbcDummy;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TestExecuteStoredProcedure {
    private static final Logger LOGGER;

    private static final String TEST_TABLE_CUSTOMERS_CREATION = "CREATE TABLE IF NOT EXISTS customers (id INTEGER GENERATED ALWAYS AS IDENTITY(START WITH 1) PRIMARY KEY, name VARCHAR(255), age INTEGER)";
    private static final String TEST_TABLE_CITY_CREATION = "CREATE TABLE IF NOT EXISTS city (id INTEGER GENERATED ALWAYS AS IDENTITY(START WITH 1) PRIMARY KEY, name VARCHAR(255), post_code VARCHAR(255))";
    private static final String TEST_TABLE_CUSTOMERS_INSERT = "INSERT INTO customers (name, age) VALUES (?,?)";
    private static final String TEST_TABLE_CITY_INSERT = "INSERT INTO city (name, post_code) VALUES ('Guangzhou', '0000')";
    private static final String TEST_PROCEDURE_CREATION = "CREATE PROCEDURE two_res_new_customer(IN i_name VARCHAR(255), IN i_age INTEGER, OUT o_id INTEGER) MODIFIES SQL DATA DYNAMIC RESULT SETS 2 BEGIN ATOMIC DECLARE city_res CURSOR WITH RETURN FOR SELECT * FROM city FOR READ ONLY; DECLARE cus_res CURSOR WITH RETURN FOR SELECT * FROM customers FOR READ ONLY; INSERT INTO customers (name, age) VALUES (i_name, i_age); SET o_id = IDENTITY(); UPDATE city SET post_code = '0001' WHERE id = 1; OPEN cus_res; OPEN city_res; END";
    private static final String TEST_PROCEDURE_CALL = "{CALL two_res_new_customer(?,?,?)}";
    private static final String TEST_TABLE_TRUNCATE = "TRUNCATE TABLE customers";
    private static final String TEST_TABLE_CITY_TRUNCATE = "TRUNCATE TABLE city";

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.io.nio", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.lab.nice.nifi.invoker.ExecuteStoredProcedure", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.lab.nice.nifi.invoker.TestExecuteStoredProcedure", "debug");
        LOGGER = LoggerFactory.getLogger(TestExecuteStoredProcedure.class);
    }

    private TestRunner runner;

    @BeforeClass
    public static void setupClass() {
        //System.setProperty("derby.stream.error.file", "target/derby.log");
    }

    @Before
    public void setup() throws InitializationException, SQLException {
        final DBCPService dbcp = new DBCPServiceSimpleImpl();
        final Map<String, String> dbcpProperties = new HashMap<>();

        runner = TestRunners.newTestRunner(ExecuteStoredProcedure.class);
        runner.addControllerService("dbcp", dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(ExecuteStoredProcedure.DBCP_SERVICE, "dbcp");

        // load test data to database
        final Connection connection = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        JdbcDummy.execute(connection, TEST_TABLE_CITY_CREATION);
        JdbcDummy.execute(connection, TEST_TABLE_CITY_INSERT);
        JdbcDummy.execute(connection, TEST_TABLE_CUSTOMERS_CREATION);
        final List<Map<Integer, Object>> parameterList = new ArrayList<>();
        final Map<Integer, Object> parameters = new HashMap<>();
        parameters.put(1, "Will");
        parameters.put(2, 18);
        parameterList.add(parameters);
        JdbcDummy.executeBatch(connection, TEST_TABLE_CUSTOMERS_INSERT, parameterList);
        LOGGER.info("test data loaded");
    }

    @After
    public void clean() throws SQLException {
        final Connection connection = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        //JdbcDummy.execute(connection, TEST_TABLE_TRUNCATE);
        //JdbcDummy.execute(connection, TEST_TABLE_CITY_TRUNCATE);
    }

    @Test
    public void testNoIncoming() throws SQLException, ClassNotFoundException, IOException, InitializationException {
        final Connection connection = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        JdbcDummy.execute(connection, TEST_PROCEDURE_CREATION);
        runner.setIncomingConnection(false);
        runner.setProperty(ExecuteStoredProcedure.STORED_PROCEDURE_STATEMENT, TEST_PROCEDURE_CALL);
        runner.setProperty("procedure.args.in.1.type", "12");
        runner.setProperty("procedure.args.in.1.value", "Tom");
        runner.setProperty("procedure.args.in.2.type", "4");
        runner.setProperty("procedure.args.in.2.value", "20");
        runner.setProperty("procedure.args.out.3.type", "4");
        runner.setProperty("procedure.args.out.3.name", "ID");
        invokeOnTrigger(null, null, false, null, false);
    }

    public void invokeOnTrigger(final Integer queryTimeout, final String query, final boolean incomingFlowFile, final Map<String, String> attrs, final boolean setQueryProperty)
            throws InitializationException, ClassNotFoundException, SQLException, IOException {

        if (queryTimeout != null) {
            runner.setProperty(ExecuteStoredProcedure.PROCEDURE_EXECUTION_TIMEOUT, queryTimeout.toString() + " secs");
        }

        if (incomingFlowFile) {
            // incoming FlowFile content is not used, but attributes are used
            final Map<String, String> attributes = (attrs == null) ? new HashMap<>() : attrs;
            if (!setQueryProperty) {
                runner.enqueue(query.getBytes(), attributes);
            } else {
                runner.enqueue("Hello".getBytes(), attributes);
            }
        }

        if (setQueryProperty) {
            runner.setProperty(ExecuteStoredProcedure.STORED_PROCEDURE_STATEMENT, query);
        }

        runner.run();
        runner.assertAllFlowFilesTransferred(ExecuteStoredProcedure.REL_SUCCESS, 1);
        runner.assertAllFlowFilesContainAttribute(ExecuteStoredProcedure.REL_SUCCESS, ExecuteStoredProcedure.PROCEDURE_EXECUTE_DURATION);
        runner.assertAllFlowFilesContainAttribute(ExecuteStoredProcedure.REL_SUCCESS, ExecuteStoredProcedure.PROCEDURE_RETURN_RESULTSET_COUNT);
        runner.assertAllFlowFilesContainAttribute(ExecuteStoredProcedure.REL_SUCCESS, ExecuteStoredProcedure.PROCEDURE_RETURN_ROW_COUNT);
        runner.assertAllFlowFilesContainAttribute(ExecuteStoredProcedure.REL_SUCCESS, ExecuteStoredProcedure.PROCEDURE_RETURN_OUTPUT_COUNT);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStoredProcedure.REL_SUCCESS);

        final ObjectMapper objectMapper = new ObjectMapper();
        for (MockFlowFile flowFile : flowFiles) {
            final WrapInt resultCount = new WrapInt("ResultsCount");
            final WrapInt rowCount = new WrapInt("RowCount");
            final WrapInt outputCount = new WrapInt("OutputCount");
            final JsonNode root = objectMapper.readTree(flowFile.toByteArray());
            LOGGER.info("{}", root);
            final JsonNode results = root.path("Results");
            final JsonNode outputs = root.path("Outputs");
            if (!results.isMissingNode()) {
                results.elements().forEachRemaining(node -> {
                    if (node.isArray()) {
                        node.forEach(row -> {
                            LOGGER.info("Row: {}", row);
                            rowCount.increase();
                        });
                    } else {
                        LOGGER.info("UpdateCount: {}", node);
                    }
                    resultCount.increase();
                });
            }
            if (!outputs.isMissingNode()) {
                outputs.forEach(o -> {
                    LOGGER.info("Output: {}", o);
                    outputCount.increase();
                });
            }
            LOGGER.info("{}, {}, {}", resultCount, rowCount, outputCount);
        }
    }

    /**
     * Simple implementation only for ExecuteSQL processor testing.
     */
    class DBCPServiceSimpleImpl extends AbstractControllerService implements DBCPService {

        @Override
        public String getIdentifier() {
            return "dbcp";
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                Class.forName("org.hsqldb.jdbc.JDBCDriver");
                final Connection con = DriverManager.getConnection("jdbc:hsqldb:mem:test", "test", "");
                return con;
            } catch (final Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }

    static class WrapInt {
        private final String name;
        private int value = 0;

        WrapInt(final String name) {
            this.name = name;
        }

        void increase() {
            this.value++;
        }

        String getName() {
            return name;
        }

        int getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WrapInt wrapInt = (WrapInt) o;
            return value == wrapInt.value &&
                    Objects.equals(name, wrapInt.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, value);
        }

        @Override
        public String toString() {
            return "{" +
                    "name='" + name + '\'' +
                    ", value=" + value +
                    '}';
        }
    }
}
