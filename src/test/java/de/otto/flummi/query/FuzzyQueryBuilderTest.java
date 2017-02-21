package de.otto.flummi.query;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.testng.annotations.Test;

import static de.otto.flummi.request.GsonHelper.object;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class FuzzyQueryBuilderTest {
	@Test
	public void shouldCreateFullQuery() throws Exception {
		JsonObject query = new FuzzyQueryBuilder("someField", "someValue").boost(3).fuzziness(2).maxExpansions(20).prefixLength(2).build();

		assertThat(query, is(object("fuzzy", object(
						"someField", object(
								"value", new JsonPrimitive("someValue"),
								"boost", new JsonPrimitive(3),
								"fuzziness", new JsonPrimitive(2),
								"max_expansions", new JsonPrimitive(20),
								"prefix_length", new JsonPrimitive(2)
						)
				)
		)));
	}

	@Test
	public void shouldCreateQueryWithDefaultOptions() throws Exception {
		JsonObject query = new FuzzyQueryBuilder("someField", "someValue").build();

		assertThat(query, is(object("fuzzy", object(
						"someField", object(
								"value", new JsonPrimitive("someValue")
						)
				)
		)));
	}
}