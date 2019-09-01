package lab.nice.nifi.invoker;

import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

public class StoredProcedureInvokerProcessor extends AbstractProcessor {
    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        getLogger().info("Hello World.");
    }
}
