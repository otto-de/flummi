package de.otto.flummi.response;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;

public class AnalyzeResponse  {
    private final List<Token> tokens;

    public AnalyzeResponse(List<Token> tokens) {
        this.tokens = tokens;
    }

    public List<Token> getTokens(){
        return tokens;
    };

    public static Builder builder() {
        return new Builder();
    }

    public static AnalyzeResponse emptyResponse() {
        return new AnalyzeResponse(emptyList());
    }

    public static final class Builder {
        private List<Token> tokens = new ArrayList<>();

        public Builder setHits(List<Token> tokens) {
            this.tokens = tokens;
            return this;
        }

        public AnalyzeResponse build() {
            return new AnalyzeResponse(tokens);
        }
    }
}

