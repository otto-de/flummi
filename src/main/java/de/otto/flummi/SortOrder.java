package de.otto.flummi;

public enum SortOrder {
    /**
     * Ascending order.
     */
    ASC {
        @Override
        public String toString() {
            return "asc";
        }
    },
    /**
     * Descending order.
     */
    DESC {
        @Override
        public String toString() {
            return "desc";
        }
    }
}