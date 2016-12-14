package de.otto.flummi.domain.index;

public enum IndexCodec {
    DEFAULT,
    BEST_COMPRESSION,
    ;

    public String toString() {
        return super.toString().toLowerCase();
    }
}
