package ipiad.parser.services;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import ipiad.parser.model.UrlModel;
import ipiad.parser.misc_functions.Requests;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

public class MainPageLinksGetter extends Thread {
    private final String initialLink;
    private final ConnectionFactory rmqFactory;
    private static final Logger logger = LoggerFactory.getLogger(MainPageLinksGetter.class);

    public MainPageLinksGetter(String initialLink, ConnectionFactory rmqFactory) {
        this.initialLink = initialLink;
        this.rmqFactory = rmqFactory;
    }

    @Override
    public void run() {
        try {
            Connection connection = rmqFactory.newConnection();
            Channel channel = connection.createChannel();
            logger.info("Connected to RabbitMQ link queue");
            crawlAndExtractLinks(initialLink, channel);
            channel.close();
            connection.close();
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private void crawlAndExtractLinks(String url, Channel channel) throws IOException {
        logger.info("Starting parsing: " + url);
        String baseUrl = "https://www.kommersant.ru";
        Optional<Document> documentOpt = Requests.requestWithRetry(url);
        if (documentOpt.isPresent()) {
            Document originalDocument = documentOpt.get();
            for (Element publicationItem : originalDocument.select("div.rubric_lenta__item_text")) {
                Element aWithHref = publicationItem.children().select("h2.rubric_lenta__item_name").first().select("a.uho__link").first();
                try {
                    String href = baseUrl + aWithHref.attributes().get("href");
                    String title = aWithHref.children().select("span.vam").first().text();
                    UrlModel extractedUrl = new UrlModel(href, title);
                    logger.debug(extractedUrl.toJsonString());
                    channel.basicPublish("", Requests.RMQ_CHAN_LINKS, null, extractedUrl.toJsonString().getBytes());
                } catch (Exception e) {
                    System.out.println(e);
                    logger.info(e.getMessage());
                }
            }
        }
        logger.info("Finished parsing:" + url);
    }
}