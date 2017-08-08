package de.otto.flummi.query;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import static de.otto.flummi.request.GsonHelper.object;

public class MatchQueryBuilder implements QueryBuilder {
	private final String name;
	private final JsonElement value;

	public MatchQueryBuilder(String name, JsonElement value) {
		this.name = name;
		this.value = value;
	}

	@Override
	public JsonObject build() {
		if (name == null || name.isEmpty()) {
			throw new RuntimeException("missing property 'name'");
		}
		if (value == null) {
			throw new RuntimeException("missing property 'value'");
		}
		JsonObject match = object(name, value);
		return object("match", match);
	}
}
