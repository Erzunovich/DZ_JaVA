package ipiad.parser;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import ipiad.parser.elastic.EsStorage;
import ipiad.parser.services.MainPageLinksGetter;
import ipiad.parser.services.NewsPageParser;
import ipiad.parser.services.NewsModelLoader;
import ipiad.parser.misc_functions.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;



public class Main {
    private static final String url = "https://www.kommersant.ru/archive/news";
    private static final String INDEX_NAME = "news";
    private static final String ES_URL = "http://localhost:9200";
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    public static void main(String[] args) throws InterruptedException, IOException, TimeoutException {
        logger.info("Start service");
        ConnectionFactory rmqConFactory = new ConnectionFactory();
        prepareRMQ(rmqConFactory);
        EsStorage esStorage = new EsStorage(ES_URL, INDEX_NAME);
        esStorage.createIndexIfNE();

        // запускаем основное приложение
        MainPageLinksGetter mainPageLinksGetter = new MainPageLinksGetter(url, rmqConFactory);

        NewsPageParser newsPageParserThread1 = new NewsPageParser(rmqConFactory, esStorage);
        NewsPageParser newsPageParserThread2 = new NewsPageParser(rmqConFactory, esStorage);
        NewsPageParser newsPageParserThread3 = new NewsPageParser(rmqConFactory, esStorage);

        NewsModelLoader newsModelLoaderThread1 = new NewsModelLoader(rmqConFactory, esStorage);
        NewsModelLoader newsModelLoaderThread2 = new NewsModelLoader(rmqConFactory, esStorage);


        mainPageLinksGetter.start();

        newsPageParserThread1.start();
        newsPageParserThread2.start();
        newsPageParserThread3.start();

        newsModelLoaderThread1.start();
        newsModelLoaderThread2.start();


        mainPageLinksGetter.join();

        newsPageParserThread1.join();
        newsPageParserThread2.join();
        newsPageParserThread3.join();

        newsModelLoaderThread1.join();
        newsModelLoaderThread2.join();
    }

    private static void prepareRMQ(ConnectionFactory rmqConFactory) throws IOException, TimeoutException {
        rmqConFactory.setHost("127.0.0.1");
        rmqConFactory.setPort(5672);
        rmqConFactory.setVirtualHost("/");
        rmqConFactory.setUsername("rabbitmq");
        rmqConFactory.setPassword("rabbitmq");

        Connection connection = rmqConFactory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(Requests.RMQ_CHAN_LINKS, false, false, false, null);
        channel.queueDeclare(Requests.RMQ_CHAN_NEWS, false, false, false, null);
        channel.close();
        connection.close();
    }
}