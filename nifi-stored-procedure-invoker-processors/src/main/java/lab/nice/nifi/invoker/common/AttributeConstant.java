package lab.nice.nifi.invoker.common;

import java.util.regex.Pattern;

public final class AttributeConstant {

    public static final String PROCEDURE_TYPE_IN = "in";
    public static final String PROCEDURE_TYPE_OUT = "out";
    public static final String PROCEDURE_TYPE_INOUT = "inout";

    public static final String PROCEDURE_VALUE_TEMPLATE = "procedure.args.%s.%d.value";
    public static final String PROCEDURE_FORMAT_TEMPLATE = "procedure.args.%s.%d.format";
    public static final String PROCEDURE_NAME_TEMPLATE = "procedure.args.%s.%d.name";

    public static final Pattern PROCEDURE_TYPE_ATTRIBUTE = Pattern.compile("procedure\\.args\\.(in|out|inout)\\.(\\d+)\\.type");

    private AttributeConstant() {

    }
}
