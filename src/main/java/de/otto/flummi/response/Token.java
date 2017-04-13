package de.otto.flummi.response;

public class Token {
    private final String token;
    private final String type;
    private final Integer position;
    private final Integer startOffset;
    private final Integer endOffset;

    public Token(String token, String type, Integer position, Integer startOffset, Integer endOffset) {
        this.token = token;
        this.type = type;
        this.position = position;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public String getToken() {
        return token;
    }

    public String getType() {
        return type;
    }

    public Integer getPosition() {
        return position;
    }

    public Integer getEndOffset() {
        return endOffset;
    }

    public Integer getStartOffset() {
        return startOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Token token1 = (Token) o;

        if (token != null ? !token.equals(token1.token) : token1.token != null) return false;
        if (type != null ? !type.equals(token1.type) : token1.type != null) return false;
        if (position != null ? !position.equals(token1.position) : token1.position != null) return false;
        if (endOffset != null ? !endOffset.equals(token1.endOffset) : token1.endOffset != null) return false;
        return startOffset != null ? startOffset.equals(token1.startOffset) : token1.startOffset == null;
    }

    @Override
    public int hashCode() {
        int result = token != null ? token.hashCode() : 0;
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (position != null ? position.hashCode() : 0);
        result = 31 * result + (endOffset != null ? endOffset.hashCode() : 0);
        result = 31 * result + (startOffset != null ? startOffset.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Token{");
        sb.append("token='").append(token).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", position=").append(position);
        sb.append(", startOffset=").append(startOffset);
        sb.append(", endOffset=").append(endOffset);
        sb.append('}');
        return sb.toString();
    }
}

