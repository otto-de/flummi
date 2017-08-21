# Release History

## release 2.0.27.0
* Add support for match queries

## release 2.0.26.0
* Add support for retrieving index mapping. 

## release 2.0.25.0
* Add support for geo_distance queries

## release 2.0.24.2
* make BoolQueryBuilder.isEmpty to not throw NPE

## release 2.0.24.1
* add simple query to query builders

## release 2.0.24.0
* Add boosting query builder
* Implement boosting for term queries
* Implement "should" filter for bool queries - backport to 2.x

## release 2.0.23.0
* refactor TermsBuilder

## release 2.0.22.0
* Added support for resetting selected fields
* update dependencies

## release 2.0.21.0
* use new version schema, first '2' is for elastic search 2.x support

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