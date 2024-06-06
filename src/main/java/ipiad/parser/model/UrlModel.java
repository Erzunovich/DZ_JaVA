package ipiad.parser.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class UrlModel {
    private String url;
    private String title;
    private String hash;

    public UrlModel(String url, String title) {
        this.url = url;
        this.title = title;
        hash = Integer.toHexString(url.hashCode());
    }

    public UrlModel(){}

    public String getUrl() {
        return url;
    }

    public String getTitle() {
        return title;
    }

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    public String toJsonString() throws JsonProcessingException {
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        return ow.writeValueAsString(this);
    }

    public void  objectFromStrJson(String jsonData) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(jsonData);
        this.url = node.get("url").asText();
        this.title = node.get("title").asText();
        this.hash = node.get("hash").asText();
    }

    @Override
    public String toString() {
        return "UrlModel{" +
                "url='" + url + '\'' +
                ", title='" + title + '\'' +
                ", hash='" + hash + '\'' +
                '}';
    }
}
