package de.otto.flummi.query;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.testng.annotations.Test;

import static de.otto.flummi.request.GsonHelper.object;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;


public class GeoDistanceQueryBuilderTest {

	@Test
	public void testName() throws Exception {
		JsonObject queryAsJson = new GeoDistanceQueryBuilder("position").setDistance("47 km").setLat(53.1).setLon(9.7).build();

		assertThat(queryAsJson, is(
				object("geo_distance", object(
						"distance", new JsonPrimitive("47 km"),
						"position", object(
								"lat", new JsonPrimitive(53.1),
								"lon", new JsonPrimitive(9.7)
						)
						)
				)));
	}
}