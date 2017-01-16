package de.otto.flummi.aggregations;

import java.util.ArrayList;
import java.util.List;

public abstract class SubAggregationBuilder<T extends SubAggregationBuilder<T>> implements AggregationBuilder {
    private final String name;
    protected List<AggregationBuilder> subAggregations;

    protected SubAggregationBuilder(String name) {
        this.name = name;
    }

    public T subAggregation(AggregationBuilder subAggregation) {
        if (subAggregations == null) {
            subAggregations = new ArrayList<>();
        }
        subAggregations.add(subAggregation);

        return (T) this;
    }

    @Override
    public String getName() {
        return name;
    }

}
