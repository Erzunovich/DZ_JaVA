    package ipiad.parser.elastic;

    import co.elastic.clients.elasticsearch.ElasticsearchClient;
    import co.elastic.clients.json.jackson.JacksonJsonpMapper;
    import co.elastic.clients.transport.ElasticsearchTransport;
    import co.elastic.clients.transport.endpoints.BooleanResponse;
    import co.elastic.clients.transport.rest_client.RestClientTransport;
    import com.fasterxml.jackson.databind.ObjectMapper;
    import com.fasterxml.jackson.databind.json.JsonMapper;
    import ipiad.parser.model.NewsModel;
    import ipiad.parser.services.NewsModelLoader;
    import org.apache.http.HttpHost;
    import org.elasticsearch.client.RestClient;
    import org.slf4j.Logger;
    import org.slf4j.LoggerFactory;

    import java.io.IOException;

    public class EsStorage {
        private final ObjectMapper objectMapper;
        private final RestClient restClient;
        private final ElasticsearchTransport transport;
        private final ElasticsearchClient elasticClient;
        private final String indexName;
        private final static Logger log = LoggerFactory.getLogger(NewsModelLoader.class);

        public EsStorage(String elasticUrl, String idxName) {
            objectMapper = JsonMapper.builder().build();
            restClient = RestClient.builder(HttpHost.create(elasticUrl)).build();
            transport = new RestClientTransport(restClient, new JacksonJsonpMapper(objectMapper));
            elasticClient = new ElasticsearchClient(transport);
            indexName = idxName;
            log.info("Elastic connection established!");
        }

        public void createIndexIfNE() throws IOException {
            BooleanResponse indexExists = elasticClient.indices().exists(ex -> ex.index(indexName));
            log.info(elasticClient.indices().stats().toString());
            if (indexExists.value()) {
                log.info("Index already exists: " + indexName);
                return;
            }
            elasticClient.indices().create(c -> c.index(indexName).mappings(m -> m
                    .properties("id", p -> p.text(d -> d.fielddata(true)))
                    .properties("header", p -> p.text(d -> d.fielddata(true)))
                    .properties("subHeader", p -> p.text(d -> d.fielddata(true)))
                    .properties("theme", p -> p.text(d -> d.fielddata(true)))
                    .properties("text", p -> p.text(d -> d.fielddata(true)))
                    .properties("URL", p -> p.text(d -> d.fielddata(true)))
                    .properties("time", p -> p.text(d -> d.fielddata(true)))
                    .properties("hash", p -> p.text(d -> d.fielddata(true)))
            ));
            log.info("Created index: " + indexName);
        }

        public void insertNewsModel(NewsModel news) {
            try {
                elasticClient.index(i -> i.index(indexName).document(news));
                log.info("News model from url: " + news.getURL() + " was added to es");
            } catch (IOException e) {
                log.error("Failed add news model for url {} , err: {}", news.getURL(), e.getMessage());
            }
        }

        public boolean checkAlreadyExists(String hashValue) {
            try {
                return elasticClient.search(s -> s
                                .index(indexName)
                                .query(q -> q.match(t -> t.field("hash").query(hashValue))),
                        NewsModel.class
                ).hits().total().value() != 0;
            } catch (IOException e) {
                log.error("Failed check value is es for hash: {} , err: {}", hashValue, e.getMessage());
                return false;
            }
        }
    }