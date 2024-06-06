package ipiad.parser.misc_functions;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.jsoup.nodes.Document;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

public class Requests {
    private static final int RETRY_COUNT = 5;
    private static final int TIME_WAIT_MS = 3000;
    public static final String RMQ_CHAN_LINKS = "parser_links";
    public static final String RMQ_CHAN_NEWS = "parser_news";
    private static final Logger logger = LoggerFactory.getLogger(Requests.class);

    public static Optional<Document> requestWithRetry(String url) {
        Optional<Document> doc = Optional.empty();
        for (int attempt = 0; attempt < RETRY_COUNT; attempt++) {
            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                final HttpGet httpGet = new HttpGet(url);
                try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                    int statusCode = response.getStatusLine().getStatusCode();
                    switch (statusCode) {
                        case 200: {
                            HttpEntity entity = response.getEntity();
                            doc = Optional.of(Jsoup.parse(entity.getContent(), null, url));
                            return doc;
                        }
                        case 404:
                            logger.info("Received 404 for " + url);
                            break;
                        case 429: {
                            logger.info("Received 429 (too many requests) for " + url,", sleep for " + TIME_WAIT_MS +" ms");
                            response.close();
                            httpClient.close();
                            Thread.sleep(TIME_WAIT_MS);
                            break;
                        }
                        default:
                            logger.warn("Received unexpected status code: {}, will retry {}", statusCode, attempt+1);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } catch (IOException e) {
                logger.error("Request error:", e);
            }
        }

        return doc;
    }
}