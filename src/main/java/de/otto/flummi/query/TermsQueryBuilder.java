package de.otto.flummi.query;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.util.List;

import static de.otto.flummi.GsonCollectors.toJsonArray;

public class TermsQueryBuilder implements QueryBuilder{
    private final String name;
    private final List<String> terms;

    public TermsQueryBuilder(String name, List<String> terms) {
        this.name = name;
        this.terms = terms;
    }

    @Override
    public JsonObject build() {
        if (name==null || name.isEmpty()) {
            throw new RuntimeException("missing property 'name'");
        }
        if (terms == null || terms.isEmpty()) {
            throw new RuntimeException("missing property 'terms'");
        }
        JsonObject jsonObject = new JsonObject();
        JsonObject termsObject = new JsonObject();
        jsonObject.add("terms", termsObject);
        termsObject.add(name, terms.stream().map(JsonPrimitive::new).collect(toJsonArray()));
        return jsonObject;
    }

}
