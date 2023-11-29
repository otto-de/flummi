# Release History
## Release 8.0.0.7-SNAPSHOT
* Use SynchronizedList in BulkRequestBuilder

## Release 8.0.0.6
* Set type attribute optional in DeleteActionBuilder

* ## Release 8.0.0.4
* Set type attribute optional in DeleteRequestBuilder

## Release 8.0.0.3
* Rebuild with JDK8

## Release 8.0.0.2
* Add IndexSettingsBuilder

## Release 8.0.0.0
* Support for ElasticSearch 8.x
** _type isn't supported in ES8, see https://www.elastic.co/guide/en/elasticsearch/reference/current/migrating-8.0.html#breaking-changes-8.0

## Release 7.0.0.3
* add support for authorization header - previous upload did not include flummi authorization constructor

## Release 7.0.0.2
* add support for authorization header

## Release 7.0.0.1
* Previous upload did accidentally not include the merge for ES7

## Release 7.0.0.0
* Support for ElasticSearch 7

## release 6.0.0.0
* Support for ElasticSearch 6
* use async-http-client 2.4.4

## release 5.0.32.0
* use version of async-http-client 2.0.37

## release 5.0.31.0
* add support for min_doc_count

## release 5.0.30.0
* add support for index mapping

## release 5.0.29.0
* add support for nested aggregations

## release 5.0.28.1
* remove deprecated in IndexRequestBuilder

## release 5.0.28.0
* Add support for geo_distance queries
* don't perform http request when no indices are given for deletion, throw Exception instead

## release 5.0.27.0
* add support for _analyze API

## release 5.0.26.5
* add forcemerge request to IndicesAdminClient

## release 5.0.26.4
* add ForceMergeRequestBuilder

## release 5.0.26.3
* Add query string as message to execution exception

## release 5.0.26.2
* Replace filtered query with bool query because filtered is removed in ES 5.0.

## release 5.0.26.1
* add simple query to query builders
* make BoolQueryBuilder.isEmpty to not throw NPE

## release 5.0.26.0
* Add boosting query builder
* Implement boosting for term queries
* Implement "should" filter for bool queries

## release 5.0.25.1
* Fix NPE in SearchRequestBuilder https://github.com/otto-de/flummi/issues/9

## release 5.0.25.0
* prevent NPE in FieldSortBuilder if no sort order exists
* SumAggregationBuilder introduced

## release 5.0.24.0
* update gson dependency
* refactor TermsQuery api

## release 5.0.23.0
* use new version schema, 5 is for es 5.x support
* Upgrade dependencies

## release 0.22.0
* Add term query builder for types other than string
* Add support for Elasticsearch 5.x stored fields and source filters

## release 0.21.0
* rolling indexing behaviour introduced.

## release 0.20.2
* Added regex and wildcard query builders
* Implemented Pair object to get rid of JavaFX dependency

## release 0.20.1
* Improve TermsQueryBuilder, so that it is easy to specify multiple terms
* Add possibility to build an and-query from a list of queries
* Use gradle-wrapper 2.0 from public repository

## release 0.20.0
* initial public version
