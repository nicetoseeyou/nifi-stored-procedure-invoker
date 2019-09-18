package lab.nice.nifi.invoker.common;

public enum ParameterType {
    IN(ParameterTypes.PROCEDURE_TYPE_IN),
    OUT(ParameterTypes.PROCEDURE_TYPE_OUT),
    INOUT(ParameterTypes.PROCEDURE_TYPE_INOUT);

    private String type;

    ParameterType(final String type) {
        this.type = type;
    }

    public String getName() {
        return type;
    }
}
