package lab.nice.nifi.dbcp.util;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;

public final class HikariCPCommons {

    public static final AllowableValue BOOLEAN_TRUE = new AllowableValue("true", "true");
    public static final AllowableValue BOOLEAN_FALSE = new AllowableValue("false", "false");

    public static final PropertyDescriptor AUTO_COMMIT = new PropertyDescriptor.Builder()
            .name("autoCommit")
            .description("This property controls the default auto-commit behavior of connections returned from the pool. " +
                    "It is a boolean value. Default: true")
            .defaultValue(BOOLEAN_FALSE.getValue())
            .required(false)
            .allowableValues(BOOLEAN_TRUE, BOOLEAN_FALSE)
            .build();

    public static final PropertyDescriptor CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("connectionTimeout")
            .description("This property controls the maximum number of milliseconds that a client will wait for a connection from the pool. " +
                    "If this time is exceeded without a connection becoming available, a SQLException will be thrown. " +
                    "Lowest acceptable connection timeout is 250 ms. Default: 30000 (30 seconds).")
            .defaultValue(null)
            .required(false)
            .addValidator(StandardValidators.createLongValidator(250, Long.MAX_VALUE, true))
            .build();

    public static final PropertyDescriptor IDLE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("idleTimeout")
            .description("This property controls the maximum amount of time that a connection is allowed to sit idle in the pool. " +
                    "This setting only applies when minimumIdle is defined to be less than maximumPoolSize. " +
                    "Idle connections will not be retired once the pool reaches minimumIdle connections. " +
                    "Whether a connection is retired as idle or not is subject to a maximum variation of +30 seconds, " +
                    "and average variation of +15 seconds. A connection will never be retired as idle before this timeout. " +
                    "A value of 0 means that idle connections are never removed from the pool. " +
                    "The minimum allowed value is 10000ms (10 seconds). Default: 600000 (10 minutes)")
            .defaultValue(null)
            .required(false)
            .addValidator(StandardValidators.createLongValidator(10000, Long.MAX_VALUE, true))
            .build();

    public static final PropertyDescriptor MAX_LIFE_TIME = new PropertyDescriptor.Builder()
            .name("maxLifetime")
            .description("This property controls the maximum lifetime of a connection in the pool. " +
                    "An in-use connection will never be retired, only when it is closed will it then be removed. " +
                    "On a connection-by-connection basis, minor negative attenuation is applied " +
                    "to avoid mass-extinction in the pool. We strongly recommend setting this value, " +
                    "and it should be several seconds shorter than any database or infrastructure imposed " +
                    "connection time limit. A value of 0 indicates no maximum lifetime (infinite lifetime), " +
                    "subject of course to the idleTimeout setting. Default: 1800000 (30 minutes)")
            .defaultValue(null)
            .required(false)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONNECTION_TEST_QUERY = new PropertyDescriptor.Builder()
            .name("connectionTestQuery")
            .description("If your driver supports JDBC4 we strongly recommend not setting this property. " +
                    "This is for \"legacy\" drivers that do not support the JDBC4 Connection.isValid() API. " +
                    "This is the query that will be executed just before a connection is given to you from the pool " +
                    "to validate that the connection to the database is still alive. Again, try running the pool " +
                    "without this property, HikariCP will log an error if your driver is not JDBC4 compliant " +
                    "to let you know. Default: none")
            .defaultValue(null)
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor MINIMUM_IDLE = new PropertyDescriptor.Builder()
            .name("minimumIdle")
            .description("This property controls the minimum number of idle connections that HikariCP tries to maintain " +
                    "in the pool. If the idle connections dip below this value and total connections in the pool" +
                    " are less than maximumPoolSize, HikariCP will make a best effort to add additional connections quickly " +
                    "and efficiently. However, for maximum performance and responsiveness to spike demands, we recommend " +
                    "not setting this value and instead allowing HikariCP to act as a fixed size connection pool. " +
                    "Default: same as maximumPoolSize")
            .defaultValue(null)
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static PropertyDescriptor MAX_POOL_SIZE = new PropertyDescriptor.Builder()
            .name("maximumPoolSize")
            .description("This property controls the maximum size that the pool is allowed to reach, including both idle " +
                    "and in-use connections. Basically this value will determine the maximum number of actual connections " +
                    "to the database backend. A reasonable value for this is best determined by your execution environment. " +
                    "When the pool reaches this size, and no idle connections are available, calls to getConnection() will " +
                    "block for up to connectionTimeout milliseconds before timing out. Default: 10")
            .defaultValue(null)
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static PropertyDescriptor POOL_NAME = new PropertyDescriptor.Builder()
            .name("poolName")
            .description("This property represents a user-defined name for the connection pool and appears mainly in logging" +
                    " and JMX management consoles to identify pools and pool configurations. Default: auto-generated")
            .defaultValue(null)
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static PropertyDescriptor INITIALIZATION_FAIL_TIMEOUT = new PropertyDescriptor.Builder()
            .name("initializationFailTimeout")
            .description("This property controls whether the pool will \"fail fast\" if the pool cannot be seeded with " +
                    "an initial connection successfully. Any positive number is taken to be the number of milliseconds to " +
                    "attempt to acquire an initial connection; the application thread will be blocked during this period. " +
                    "If a connection cannot be acquired before this timeout occurs, an exception will be thrown. " +
                    "This timeout is applied after the connectionTimeout period. If the value is zero (0), " +
                    "HikariCP will attempt to obtain and validate a connection. If a connection is obtained, " +
                    "but fails validation, an exception will be thrown and the pool not started. However, if a connection " +
                    "cannot be obtained, the pool will start, but later efforts to obtain a connection may fail. " +
                    "A value less than zero will bypass any initial connection attempt, and the pool will start " +
                    "immediately while trying to obtain connections in the background. Consequently, later efforts" +
                    " to obtain a connection may fail. Default: 1")
            .defaultValue(null)
            .required(false)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .build();


    private HikariCPCommons() {
    }
}
