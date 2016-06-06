package de.otto.elasticsearch.client.bulkactions;

public enum IndexOpType {
    INDEX("index"),
    CREATE("create"),
    UPDATE("update");

    private String opCode;

    IndexOpType(String opCode) {
        this.opCode = opCode;
    }

    public String opCode() {
        return opCode;
    }

}
