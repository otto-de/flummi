package de.otto.elasticsearch.client.query;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import de.otto.elasticsearch.client.util.StringUtils;

public class TermsQueryBuilder implements QueryBuilder{
    private final String name;
    private final JsonElement value;

    public TermsQueryBuilder(String name, JsonElement value) {
        this.name = name;
        this.value = value;
    }

    @Override
    public JsonObject build() {
        if (StringUtils.isEmpty(name)) {
            throw new RuntimeException("missing property 'name'");
        }
        if (value == null) {
            throw new RuntimeException("missing property 'value'");
        }
        JsonObject jsonObject = new JsonObject();
        JsonObject terms = new JsonObject();
        jsonObject.add("terms", terms);
        terms.add(name, value);
        return jsonObject;
    }

}
