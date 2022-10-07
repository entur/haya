package org.entur.haya;

import org.apache.camel.CamelContext;
import org.apache.camel.FluentProducerTemplate;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class HayaTask implements CommandLineRunner {

    private final CamelContext camelContext;

    public HayaTask(CamelContext camelContext) {
        this.camelContext = camelContext;
    }

    @Override
    public void run(String... args) throws Exception {
        FluentProducerTemplate fluentProducerTemplate = camelContext.createFluentProducerTemplate();
        fluentProducerTemplate.to("direct:makeCSV").request();
    }
}
