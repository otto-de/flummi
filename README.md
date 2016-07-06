Flummi Elastic Search HTTP Client
=================================

<a href="https://travis-ci.org/otto-de/flummi"><img src="https://travis-ci.org/otto-de/flummi.svg?branch=master"></img></a>

Flummi is a client library for Elastic Search 2.3. It provides a comprehensive Java query DSL API and communicates with
the Elastic Search Cluster via HTTP/JSON. It is licensed under the [Apache 2 License](http://www.apache.org/licenses/LICENSE-2.0.html).


Why should I use Flummi?
------------------------

* Flummi uses HTTP and JSON for communication with Elastic Search. Its only dependencies are Gson and AsyncHttpClient, so
  it is good for you if you don't want to have your application depend on the full ElasticSearch JAR.
* Flummi's API is as close as possible to the original Elastic Search transport client API. This makes it very easy to
  port existing client code to Flummi.
* Flummi uses the Elastic Search Scroll API for downloading large result sets as a stream of smaller pages.
* It supports parent-child relationships


Limitations
-----------

* Flummi is currently tested with Elastic Search 2.3.3 only.
* Flummi does not support cluster load balancing yet. You can use a hardware loadbalancer or HTTP Proxy such as nginx.
* Although it supports the most common query and request types, it is not yet fully feature complete. When you need a
  request or query type that is not yet supported by Flummi, please feel free to add it and send us a Pull Request!


How to use Flummi
-----------------

You can simply include Flummi in your Maven or Gradle build as follows.

For Maven users:

    <dependency>
        <groupId>de.otto</groupId>
        <artifactId>flummi</artifactId>
        <version>0.2.0</version>
    </dependency>

For gradle users:

    compile "de.otto:flummi:0.2.0"


### Getting started

For using Flummi in a Java application, initialize it as follows.

    AsyncHttpClient asyncHttpClient = new AsyncHttpClient();
    Flummi flummi = new Flummi(asyncHttpClient, "http://elasticsearch.base.url:9200");


### Using Flummi with Spring

For using Flummi in a Spring or Spring Boot application, you can add a simple `@Configuration` class for
initialization and then autowire Flummi in your beans.

    @Configuration
    public class FlummiConfiguration {

       @Bean
       public AsyncHttpClient asyncHttpClient() {
         return new AsyncHttpClient();
       }

       @Bean
       public Flummi flummi() {
         return new Flummi(asyncHttpClient(), "http://elasticsearch.base.url:9200");
       }
    }

### Creating an index

The following example creates a products index with a customized analyzer for the name property.

    JsonObject settings = GsonHelper.object(
        "analysis", GsonHelper.object(
            "analyzer", GsonHelper.object(
                "lowercase-analyzer", GsonHelper.object(
                    "tokenizer", "keyword-tokenizer",
                    "filter", "lowercase-filter"
                )
            ),
            "tokenizer", GsonHelper.object(
                "keyword-tokenizer", GsonHelper.object(
                    "type", "keyword"
                )
            ),
            "filter", GsonHelper.object(
                "lowercase-filter", GsonHelper.object(
                    "type", "lowercase"
                )
            )
        )
    );
    JsonObject mappings = GsonHelper.object(
        "products", GsonHelper.object(
            "properties", GsonHelper.object(
                "name", GsonHelper.object(
                    "type", "string",
                    "store", "yes",
                    "analyzer", "lowercase-analyzer",
                    "fields", GsonHelper.object(
                        "raw", GsonHelper.object(
                          "type", "string",
                          "index", "not_analyzed"
                        )
                    )
                ),
                "color", GsonHelper.object(
                    "type", "string",
                    "store", "no",
                    "index", "not_analyzed"
                )
            )
        )
    );

    flummi.admin().indices()
       .prepareCreate("products")
       .setSettings(settings)
       .setMappings(mappings)
       .execute();


### Indexing documents

A simple example that adds a product to the products index

    JsonObject bouncingBall1 = GsonHelper.object(
       "name", "Bouncing Ball small",
       "color", "green"
       );

    flummi.prepareIndex()
        .setId("bblsml-4710")
        .setSource(bouncingBall1)
        .setIndexName("products")
        .setDocumentType("product")
        .execute();


### Bulk Requests

A [bulk request](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html)
is a single HTTP request that contains multiple actions. For indexing large amounts of data, this is
much more efficient than sending one request for every document. The following simple example adds some products to the
product index using a Bulk Request

    JsonObject bouncingBall1 = GsonHelper.object(
       "name", "Bouncing Ball with smiley",
       "color", "yellow"
       );
    JsonObject bouncingBall2 = GsonHelper.object(
       "name", "Bouncing Ball XL extra bouncy",
       "color", "transparent"
       );

    flummi.prepareBulk()
        .add(
           new IndexActionBuilder("products")
               .setSource(bouncingBall1)
               .setId("bblsmly-4711")
               .setType("product")
               )
        .add(
           new IndexActionBuilder("products")
               .setSource(bouncingBall2)
               .setId("bblxlxb-4712")
               .setType("product")
               )
        .execute();


### Executing Queries

A simple example that finds up to 10 yellow-colored products in the products index:

    SearchRequestBuilder searchRequestBuilder = flummi
       .prepareSearch("products")
       .setTypes("product")
       .setSize(10)
       .setQuery(
          QueryBuilders.termQuery("color", "yellow")
            .build()
       )
       .setTimeoutMillis(150);

    SearchResponse searchResponse = searchRequestBuilder.execute()

    System.out.println("Found " + searchResponse.getHits().getTotalHits() + " products");
    searchResponse.getHits()
       .stream().map(hit -> hit.getSource().get("name").getAsString())
       .forEach(name -> System.out.println("Name: " + name));


#### Streaming large result sets with the Scroll API

For streaming large result sets, Flummi uses the
[Elastic Search Scroll API](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html)
to split the result set into smaller pages and thus reduce memory usage and network bandwidth. To use it, simply
`setScroll("1m")` on your `SearchRequestBuilder` before calling `execute()`.


### Aggregation queries

The following example shows how to do simple terms bucket aggregations.

    SearchRequestBuilder searchRequestBuilder = flummi
       .prepareSearch("products")
       .setTypes("product")
       .setSize(10)
       .setQuery(
          QueryBuilders.matchAll().build()
       )
       .addAggregation(
          new TermsBuilder("Colors").field("color").size(0)
       );

    SearchResponse searchResponse = searchRequestBuilder.execute()

    AggregationResult colors = searchResponse.getAggregations().get("Colors");
    colors.getBuckets().forEach(bucket -> System.out.println(
       "Found " + bucket.getDocCount() + " " + bucket.getKey() + " products"));


Contribution Guide
------------------

You want to contribute new features to Flummi? Great!

Flummi is built using the gradle wrapper `gradlew`. After cloning the git repository, you can create an IntelliJ Idea
project file with the following command

    ./bin/gradlew idea

Before you push, you might want to run all the unit tests with the following command

    ./bin/gradlew clean check

And don't forget to send us your pull request!
