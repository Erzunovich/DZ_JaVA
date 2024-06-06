package ipiad.parser.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.util.UUID;


public class NewsModel {
    private String id;
    private String theme;
    private String header;
    private String subHeader;
    private String text;
    private String URL;
    private String time;
    private String hash;

    public NewsModel(String header, String subHeader, String text, String URL, String time, String theme, String hash) {
        this.id = UUID.randomUUID().toString();
        this.header = header;
        this.subHeader = header;
        this.text = text;
        this.URL = URL;
        this.time = time;
        this.theme = theme;
        this.hash = hash;
    }

    public NewsModel(){}

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public String getSubHeader() {
        return subHeader;
    }

    public void setSubHeader(String subHeader) {
        this.subHeader = subHeader;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getURL() {
        return URL;
    }

    public void setURL(String URL) {
        this.URL = URL;
    }

    public String getTheme() {
        return this.theme;
    }

    public void setTheme(String theme) {
        this.theme = theme;
    }


    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    public void objectFromStrJson(String jsonData) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(jsonData);
        this.id = node.get("id").asText();
        this.header = node.get("header").asText();
        this.subHeader = node.get("subHeader").asText();
        this.text = node.get("text").asText();
        this.URL = node.get("url").asText();
        this.time = node.get("time").asText();
        this.theme = node.get("theme").asText();
        this.hash = node.get("hash").asText();
    }

    public String toJsonString() throws JsonProcessingException {
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        return ow.writeValueAsString(this);
    }

    @Override
    public String toString() {
        return "NewsModel{" +
                "id='" + id + '\'' +
                ", header='" + header + '\'' +
                ", text='" + text + '\'' +
                ", URL='" + URL + '\'' +
                ", time='" + time + '\'' +
                ", theme='" + theme + '\'' +
                ", hash='" + hash + '\'' +
                '}';
    }
}
