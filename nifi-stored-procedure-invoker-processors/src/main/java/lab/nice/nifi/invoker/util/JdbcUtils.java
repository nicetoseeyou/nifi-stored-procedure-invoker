package lab.nice.nifi.invoker.util;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.regex.Pattern;

public final class JdbcUtils {
    private static final Pattern LONG_PATTERN = Pattern.compile("^-?\\d{1,19}$");

    private JdbcUtils() {

    }

    /**
     * Determines how to map the given value to the appropriate JDBC data type and sets the parameter on the
     * provided PreparedStatement
     *
     * @param stmt           the PreparedStatement to set the parameter on
     * @param attrName       the name of the attribute that the parameter is coming from - for logging purposes
     * @param parameterIndex the index of the SQL parameter to set
     * @param parameterValue the value of the SQL parameter to set
     * @param jdbcType       the JDBC Type of the SQL parameter to set
     * @throws SQLException if the PreparedStatement throws a SQLException when calling the appropriate setter
     */
    public static void setParameter(final PreparedStatement stmt, final String attrName, final int parameterIndex, final String parameterValue, final int jdbcType,
                                    final String valueFormat)
            throws SQLException, ParseException, UnsupportedEncodingException {
        if (parameterValue == null) {
            stmt.setNull(parameterIndex, jdbcType);
        } else {
            switch (jdbcType) {
                case Types.BIT:
                    stmt.setBoolean(parameterIndex, "1".equals(parameterValue) || "t".equalsIgnoreCase(parameterValue) || Boolean.parseBoolean(parameterValue));
                    break;
                case Types.BOOLEAN:
                    stmt.setBoolean(parameterIndex, Boolean.parseBoolean(parameterValue));
                    break;
                case Types.TINYINT:
                    stmt.setByte(parameterIndex, Byte.parseByte(parameterValue));
                    break;
                case Types.SMALLINT:
                    stmt.setShort(parameterIndex, Short.parseShort(parameterValue));
                    break;
                case Types.INTEGER:
                    stmt.setInt(parameterIndex, Integer.parseInt(parameterValue));
                    break;
                case Types.BIGINT:
                    stmt.setLong(parameterIndex, Long.parseLong(parameterValue));
                    break;
                case Types.REAL:
                    stmt.setFloat(parameterIndex, Float.parseFloat(parameterValue));
                    break;
                case Types.FLOAT:
                case Types.DOUBLE:
                    stmt.setDouble(parameterIndex, Double.parseDouble(parameterValue));
                    break;
                case Types.DECIMAL:
                case Types.NUMERIC:
                    stmt.setBigDecimal(parameterIndex, new BigDecimal(parameterValue));
                    break;
                case Types.DATE:
                    Date date;

                    if (valueFormat.equals("")) {
                        if (LONG_PATTERN.matcher(parameterValue).matches()) {
                            date = new Date(Long.parseLong(parameterValue));
                        } else {
                            String dateFormatString = "yyyy-MM-dd";
                            SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatString);
                            java.util.Date parsedDate = dateFormat.parse(parameterValue);
                            date = new Date(parsedDate.getTime());
                        }
                    } else {
                        final DateTimeFormatter dtFormatter = getDateTimeFormatter(valueFormat);
                        LocalDate parsedDate = LocalDate.parse(parameterValue, dtFormatter);
                        date = new Date(Date.from(parsedDate.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant()).getTime());
                    }

                    stmt.setDate(parameterIndex, date);
                    break;
                case Types.TIME:
                    Time time;

                    if (valueFormat.equals("")) {
                        if (LONG_PATTERN.matcher(parameterValue).matches()) {
                            time = new Time(Long.parseLong(parameterValue));
                        } else {
                            String timeFormatString = "HH:mm:ss.SSS";
                            SimpleDateFormat dateFormat = new SimpleDateFormat(timeFormatString);
                            java.util.Date parsedDate = dateFormat.parse(parameterValue);
                            time = new Time(parsedDate.getTime());
                        }
                    } else {
                        final DateTimeFormatter dtFormatter = getDateTimeFormatter(valueFormat);
                        LocalTime parsedTime = LocalTime.parse(parameterValue, dtFormatter);
                        LocalDateTime localDateTime = parsedTime.atDate(LocalDate.ofEpochDay(0));
                        Instant instant = localDateTime.atZone(ZoneId.systemDefault()).toInstant();
                        time = new Time(instant.toEpochMilli());
                    }

                    stmt.setTime(parameterIndex, time);
                    break;
                case Types.TIMESTAMP:
                    long lTimestamp;

                    // Backwards compatibility note: Format was unsupported for a timestamp field.
                    if (valueFormat.equals("")) {
                        if (LONG_PATTERN.matcher(parameterValue).matches()) {
                            lTimestamp = Long.parseLong(parameterValue);
                        } else {
                            final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                            java.util.Date parsedDate = dateFormat.parse(parameterValue);
                            lTimestamp = parsedDate.getTime();
                        }
                    } else {
                        final DateTimeFormatter dtFormatter = getDateTimeFormatter(valueFormat);
                        TemporalAccessor accessor = dtFormatter.parse(parameterValue);
                        java.util.Date parsedDate = java.util.Date.from(Instant.from(accessor));
                        lTimestamp = parsedDate.getTime();
                    }

                    stmt.setTimestamp(parameterIndex, new Timestamp(lTimestamp));

                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    byte[] bValue;

                    switch (valueFormat) {
                        case "":
                        case "ascii":
                            bValue = parameterValue.getBytes(StandardCharsets.US_ASCII);
                            break;
                        case "hex":
                            bValue = DatatypeConverter.parseHexBinary(parameterValue);
                            break;
                        case "base64":
                            bValue = DatatypeConverter.parseBase64Binary(parameterValue);
                            break;
                        default:
                            throw new ParseException("Unable to parse binary data using the formatter `" + valueFormat + "`.", 0);
                    }

                    stmt.setBinaryStream(parameterIndex, new ByteArrayInputStream(bValue), bValue.length);

                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGNVARCHAR:
                case Types.LONGVARCHAR:
                    stmt.setString(parameterIndex, parameterValue);
                    break;
                case Types.CLOB:
                    try (final StringReader reader = new StringReader(parameterValue)) {
                        stmt.setCharacterStream(parameterIndex, reader);
                    }
                    break;
                case Types.NCLOB:
                    try (final StringReader reader = new StringReader(parameterValue)) {
                        stmt.setNCharacterStream(parameterIndex, reader);
                    }
                    break;
                default:
                    stmt.setObject(parameterIndex, parameterValue, jdbcType);
                    break;
            }
        }
    }

    private static DateTimeFormatter getDateTimeFormatter(String pattern) {
        switch (pattern) {
            case "BASIC_ISO_DATE":
                return DateTimeFormatter.BASIC_ISO_DATE;
            case "ISO_LOCAL_DATE":
                return DateTimeFormatter.ISO_LOCAL_DATE;
            case "ISO_OFFSET_DATE":
                return DateTimeFormatter.ISO_OFFSET_DATE;
            case "ISO_DATE":
                return DateTimeFormatter.ISO_DATE;
            case "ISO_LOCAL_TIME":
                return DateTimeFormatter.ISO_LOCAL_TIME;
            case "ISO_OFFSET_TIME":
                return DateTimeFormatter.ISO_OFFSET_TIME;
            case "ISO_TIME":
                return DateTimeFormatter.ISO_TIME;
            case "ISO_LOCAL_DATE_TIME":
                return DateTimeFormatter.ISO_LOCAL_DATE_TIME;
            case "ISO_OFFSET_DATE_TIME":
                return DateTimeFormatter.ISO_OFFSET_DATE_TIME;
            case "ISO_ZONED_DATE_TIME":
                return DateTimeFormatter.ISO_ZONED_DATE_TIME;
            case "ISO_DATE_TIME":
                return DateTimeFormatter.ISO_DATE_TIME;
            case "ISO_ORDINAL_DATE":
                return DateTimeFormatter.ISO_ORDINAL_DATE;
            case "ISO_WEEK_DATE":
                return DateTimeFormatter.ISO_WEEK_DATE;
            case "ISO_INSTANT":
                return DateTimeFormatter.ISO_INSTANT;
            case "RFC_1123_DATE_TIME":
                return DateTimeFormatter.RFC_1123_DATE_TIME;
            default:
                return DateTimeFormatter.ofPattern(pattern);
        }
    }
}
