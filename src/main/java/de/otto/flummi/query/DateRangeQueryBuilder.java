package de.otto.flummi.query;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

public class DateRangeQueryBuilder implements QueryBuilder{

    private boolean includeUpper;
    private OffsetDateTime toDateTime;
    private LocalDate toDate;
    private boolean includeLower;
    private OffsetDateTime fromDateTime;
    private LocalDate fromDate;
    private String fieldName;

    public DateRangeQueryBuilder(String fieldName) {
        this.fieldName = fieldName;
    }

    public DateRangeQueryBuilder lt(LocalDate to) {
        this.includeUpper = false;
        this.toDate = to;
        this.toDateTime = null;
        return this;
    }

    public DateRangeQueryBuilder lte(LocalDate to) {
        this.includeUpper = true;
        this.toDate = to;
        this.toDateTime = null;
        return this;
    }

    public DateRangeQueryBuilder gt(LocalDate from) {
        this.includeLower = false;
        this.fromDate = from;
        this.fromDateTime = null;
        return this;
    }

    public DateRangeQueryBuilder gte(LocalDate from) {
        this.includeLower = true;
        this.fromDate = from;
        this.fromDateTime = null;
        return this;
    }

    public DateRangeQueryBuilder lt(OffsetDateTime to) {
        this.includeUpper = false;
        this.toDateTime = to;
        this.toDate = null;
        return this;
    }

    public DateRangeQueryBuilder lte(OffsetDateTime to) {
        this.includeUpper = true;
        this.toDateTime = to;
        this.toDate = null;
        return this;
    }

    public DateRangeQueryBuilder gt(OffsetDateTime from) {
        this.includeLower = false;
        this.fromDateTime = from;
        this.fromDate = null;
        return this;
    }

    public DateRangeQueryBuilder gte(OffsetDateTime from) {
        this.includeLower = true;
        this.fromDateTime = from;
        this.fromDate = null;
        return this;
    }

    public JsonObject build() {
        if(toDate == null && toDateTime == null && fromDate == null && fromDateTime == null) {
            throw new RuntimeException("from and to fields are missing");
        }
        if(fieldName==null || fieldName.isEmpty()) {
            throw new RuntimeException("fieldName is missing");
        }
        JsonObject jsonObject = new JsonObject();
        JsonObject rangeObject = new JsonObject();
        jsonObject.add("range", rangeObject);
        JsonObject rangeParameters = new JsonObject();
        rangeObject.add(fieldName, rangeParameters);
        if(toDate != null) {
            rangeParameters.add((includeUpper ? "lte" : "lt"), new JsonPrimitive(toDate.format(DateTimeFormatter.ISO_DATE)));
        }
        if(toDateTime != null) {
            rangeParameters.add((includeUpper ? "lte" : "lt"), new JsonPrimitive(toDateTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)));
        }
        if(fromDate != null) {
            rangeParameters.add((includeLower ? "gte" : "gt"), new JsonPrimitive(fromDate.format(DateTimeFormatter.ISO_DATE)));
        }
        if(fromDateTime != null) {
            rangeParameters.add((includeLower ? "gte" : "gt"), new JsonPrimitive(fromDateTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)));
        }
        return jsonObject;
    }
}
