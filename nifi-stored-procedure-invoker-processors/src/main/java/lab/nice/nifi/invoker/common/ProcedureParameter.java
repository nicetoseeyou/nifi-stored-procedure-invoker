package lab.nice.nifi.invoker.common;

import java.sql.JDBCType;
import java.util.Objects;

public class ProcedureParameter {
    private String parameterType;
    private Integer parameterIndex;
    private JDBCType jdbcType;
    private String parameterValue;
    private String parameterFormat;
    private String parameterName;

    public ProcedureParameter(final String parameterType, final Integer parameterIndex) {
        this.parameterType = parameterType;
        this.parameterIndex = parameterIndex;
    }

    public String getParameterType() {
        return parameterType;
    }

    public void setParameterType(String parameterType) {
        this.parameterType = parameterType;
    }

    public Integer getParameterIndex() {
        return parameterIndex;
    }

    public void setParameterIndex(Integer parameterIndex) {
        this.parameterIndex = parameterIndex;
    }

    public JDBCType getJdbcType() {
        return jdbcType;
    }

    public void setJdbcType(Integer jdbcType) {
        this.jdbcType = JDBCType.valueOf(jdbcType);
    }

    public String getParameterValue() {
        return parameterValue;
    }

    public void setParameterValue(String parameterValue) {
        this.parameterValue = parameterValue;
    }

    public String getParameterFormat() {
        return parameterFormat;
    }

    public void setParameterFormat(String parameterFormat) {
        this.parameterFormat = parameterFormat;
    }

    public String getParameterName() {
        return parameterName;
    }

    public void setParameterName(String parameterName) {
        this.parameterName = parameterName;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ProcedureParameter that = (ProcedureParameter) o;
        return Objects.equals(parameterType, that.parameterType) &&
                Objects.equals(parameterIndex, that.parameterIndex) &&
                Objects.equals(jdbcType, that.jdbcType) &&
                Objects.equals(parameterValue, that.parameterValue) &&
                Objects.equals(parameterFormat, that.parameterFormat) &&
                Objects.equals(parameterName, that.parameterName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parameterType, parameterIndex, jdbcType, parameterValue, parameterFormat, parameterName);
    }

    @Override
    public String toString() {
        return "ProcedureParameter {" +
                "parameterType='" + parameterType + '\'' +
                ", parameterIndex=" + parameterIndex +
                ", jdbcType=" + jdbcType +
                ", parameterValue='" + parameterValue + '\'' +
                ", parameterFormat='" + parameterFormat + '\'' +
                ", parameterName='" + parameterName + '\'' +
                '}';
    }
}
