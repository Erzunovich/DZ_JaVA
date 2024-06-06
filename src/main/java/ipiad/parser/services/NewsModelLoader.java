package ipiad.parser.services;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import ipiad.parser.elastic.EsStorage;
import ipiad.parser.model.NewsModel;
import ipiad.parser.misc_functions.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;

public class NewsModelLoader extends Thread {
    private final ConnectionFactory rmqFactory;
    private final EsStorage esStorage;
    private static final Logger logger = LoggerFactory.getLogger(NewsModelLoader.class);

    public NewsModelLoader(ConnectionFactory factory, EsStorage esStorage) {
        this.rmqFactory = factory;
        this.esStorage = esStorage;
    }

    @Override
    public void run() {
        try {
            Connection connection = rmqFactory.newConnection();
            Channel channel = connection.createChannel();
            while (true) {
                GetResponse rmqResp = channel.basicGet(Requests.RMQ_CHAN_NEWS, true);
                if (rmqResp == null) {
                    continue;
                }

                String newsJson = new String(rmqResp.getBody(),UTF_8);
                NewsModel newsModel = new NewsModel();
                logger.info("Got parsed data to insert" + newsJson);
                newsModel.objectFromStrJson(newsJson);
                if (!esStorage.checkAlreadyExists(newsModel.getHash())) {
                    esStorage.insertNewsModel(newsModel);
                    logger.info("Inserted data from " + newsModel.getURL() + " into Elastic");
                } else {
                    logger.info("Url: {} was already found in es, hash: {}", newsModel.getURL(), newsModel.getHash());
                }
            }
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
