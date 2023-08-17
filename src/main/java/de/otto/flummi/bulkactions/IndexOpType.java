package de.otto.flummi.bulkactions;

public enum IndexOpType {
    INDEX("index"),
    CREATE("create"),
    UPDATE("update"),
    DELETE("delete");

    private String opCode;

    IndexOpType(String opCode) {
        this.opCode = opCode;
    }

    public String opCode() {
        return opCode;
    }

}
