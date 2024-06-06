package ipiad.parser.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.rabbitmq.client.*;
import ipiad.parser.elastic.EsStorage;
import ipiad.parser.model.NewsModel;
import ipiad.parser.model.UrlModel;
import ipiad.parser.misc_functions.Requests;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

public class NewsPageParser extends Thread {
    private final ConnectionFactory rmqFactory;
    private static final Logger logger = LoggerFactory.getLogger(NewsPageParser.class);
    private final EsStorage esStorage;

    public NewsPageParser(ConnectionFactory rmqFactory, EsStorage bridge) {
        this.rmqFactory = rmqFactory;
        this.esStorage = bridge;
    }

    @Override
    public void run() {
        try {
            Connection connection = rmqFactory.newConnection();
            Channel channel = connection.createChannel();
            while (true) {
                try {
                    if (channel.messageCount(Requests.RMQ_CHAN_LINKS) == 0) continue;
                    channel.basicConsume(Requests.RMQ_CHAN_LINKS, false, "pagesTag", new DefaultConsumer(channel) {
                        @Override
                        public void handleDelivery(String consumerTag,
                                                   Envelope envelope,
                                                   AMQP.BasicProperties properties,
                                                   byte[] body)
                                throws IOException {
                            long deliveryTag = envelope.getDeliveryTag();
                            String message = new String(body, StandardCharsets.UTF_8);
                            UrlModel url = new UrlModel();
                            url.objectFromStrJson(message);
                            try {
                                NewsModel newsModel = parsePage(url);
                                if (newsModel != null) {
                                    try {
                                        channel.basicPublish("", Requests.RMQ_CHAN_NEWS, null, newsModel.toJsonString().getBytes());
                                        logger.info("Published page in the page queue");
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                }
                            } catch (InterruptedException e) {
                                logger.info(e.getMessage());
                            }
                            channel.basicAck(deliveryTag, false);
                        }
                    });
                } catch (IndexOutOfBoundsException e) {
                    logger.info(e.getMessage());
                }
            }
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    NewsModel parsePage(UrlModel url) throws InterruptedException, JsonProcessingException {
        if (esStorage.checkAlreadyExists(url.getHash())) {
            logger.info("Url: {} already exists in elastic hash: {}" , url.getUrl(),  url.getHash());
            return null;
        }
        String urlString = url.getUrl();
        Optional<Document> document = Requests.requestWithRetry(urlString);
        if (document.isPresent()) {
            Document doc = document.get();
            String header = doc.select("h1.doc_header__name").first().text();
            String subHeader = "";
            Element subHeaderEl = doc.select("h2.doc_header__subheader").first();
            if (subHeaderEl != null) {
                subHeader = subHeaderEl.text();
            }
            String time = doc.select("time.doc_header__publish_time").text();
            String theme = doc.select("li.crumbs__item").first().select("a").first().text();

            StringBuilder textContent = new StringBuilder();
            Element divElement = doc.select("div.article_text_wrapper").first();
            for (Element pElement : divElement.select("p")) {
                textContent.append(pElement.text()).append("\n");
            }

            NewsModel news = new NewsModel(
                    header,
                    subHeader,
                    textContent.toString(),
                    urlString,
                    time,
                    theme,
                    url.getHash()
            );
            logger.debug(news.toJsonString());
            return news;
        }
        return null;
    }

}
