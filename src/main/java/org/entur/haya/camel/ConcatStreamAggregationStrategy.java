package org.entur.haya.camel;

import org.apache.camel.Exchange;
import org.apache.camel.processor.aggregate.GroupedExchangeAggregationStrategy;
import org.entur.geocoder.model.PeliasDocument;

import java.util.stream.Stream;

public class ConcatStreamAggregationStrategy extends GroupedExchangeAggregationStrategy {

    @Override
    public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
        if (oldExchange != null && newExchange != null) {
            newExchange.getIn().setBody(
                    Stream.concat(oldExchange.getIn().getBody(Stream.class), newExchange.getIn().getBody(Stream.class))
            );
            return newExchange;
        }

        return newExchange;
    }
}
