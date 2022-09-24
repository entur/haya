package org.entur.haya.csv;

public enum CSVHeader {
    ID("id"),
    INDEX("index"),
    TYPE("type"),
    NAME("name"),
    LATITUDE("lat"),
    LONGITUDE("lon"),
    ADDRESS_STREET("street"),
    ADDRESS_NUMBER("number"),
    ADDRESS_ZIP("zipcode"),
    POPULARITY("popularity"),
    CATEGORY("category_json"),
    SOURCE("source"),
    SOURCE_ID("source_id"),
    LAYER("layer"),
    PARENT("parent_json");

    private String columnName;

    CSVHeader(String columnName) {
        this.columnName = columnName;
    }

    public String columnName() {
        return columnName;
    }
}
