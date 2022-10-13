package org.entur.haya;

import org.apache.camel.CamelContext;
import org.apache.camel.FluentProducerTemplate;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

@SpringBootApplication
public class HayaApplication {

    private final CamelContext camelContext;

    public HayaApplication(CamelContext camelContext) {
        this.camelContext = camelContext;
    }

    public static void main(String[] args) {
        SpringApplication.run(HayaApplication.class, args);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void doSomethingAfterStartup() {
        FluentProducerTemplate fluentProducerTemplate = camelContext.createFluentProducerTemplate();
        fluentProducerTemplate.to("direct:makeCSV").request();
    }
}
