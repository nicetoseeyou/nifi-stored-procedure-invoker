package lab.nice.nifi.invoker.util;


import lab.nice.nifi.invoker.common.AttributeConstant;
import lab.nice.nifi.invoker.common.Parameter;

import java.util.Map;
import java.util.regex.Matcher;

public final class AttributeHandler {
    private AttributeHandler() {
    }

    /**
     * Retrieve stored procedure parameter from NiFi processor properties or NiFi FlowFile attributes.
     *
     * @param attributes          NiFi processor properties or NiFi FlowFile attributes
     * @param parameters the target stored procedure parameter map
     */
    public static void retrieveProcedureParameter(final Map<String, String> attributes,
                                                  final Map<Integer, Parameter> parameters) {
        if (null != attributes && !attributes.isEmpty()) {
            for (Map.Entry<String, String> entry : attributes.entrySet()) {
                final String attributeName = entry.getKey();
                final Matcher matcher = AttributeConstant.PROCEDURE_TYPE_ATTRIBUTE.matcher(attributeName);
                final String parameterType = matcher.group(1);
                final Integer parameterIndex = Integer.parseInt(matcher.group(2));
                final Integer jdbcType = Integer.parseInt(entry.getValue());
                final Parameter parameter =
                        parameters.getOrDefault(parameterIndex, new Parameter(parameterType, parameterIndex, jdbcType));
                if (AttributeConstant.PROCEDURE_TYPE_IN.equals(parameterType)){

                }else if (AttributeConstant.PROCEDURE_TYPE_OUT.equals(parameterType)){

                }else if (AttributeConstant.PROCEDURE_TYPE_INOUT.equals(parameterType)){

                }else {
                    //do nothing, should never be
                }
            }
        }
    }


}
