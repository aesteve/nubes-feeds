package com.github.aesteve.vertx.nubes.examples.feeds.utils.rss;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.github.aesteve.vertx.nubes.examples.feeds.services.RedisDAO;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.io.FeedException;
import com.rometools.rome.io.SyndFeedInput;

public class FeedReader {

	private final static Logger log = LoggerFactory.getLogger(FeedReader.class);

	private RedisDAO redis;
	private Map<String, HttpClient> clients;
	private JsonObject jsonFeed;
	private Vertx vertx;

	public FeedReader(Vertx vertx, RedisDAO redis, Map<String, HttpClient> clients, JsonObject jsonFeed) {
		this.vertx = vertx;
		this.redis = redis;
		this.clients = clients;
		this.jsonFeed = jsonFeed;
	}

	public void readFeed(Future<Void> future) {
		String feedUrl = jsonFeed.getString("url");
		String feedId = jsonFeed.getString("hash");
		URL url;
		log.info("Reading feed at URL : " + feedUrl);
		try {
			url = new URL(feedUrl);
		} catch (MalformedURLException mfe) {
			log.warn("Invalid url : " + feedUrl, mfe);
			future.fail(mfe);
			return;
		}

		redis.getMaxDate(feedId, maxDate -> {
			getXML(url, response -> {
				int status = response.statusCode();
				if (status < 200 || status >= 300) {
					if (future != null) {
						future.fail(new RuntimeException("Could not read feed " + feedUrl + ". Response status code : " + status));
					}
					return;
				}
				response.bodyHandler(buffer -> {
					this.parseXmlFeed(buffer, maxDate, url, feedId, future);
				});
			});
		});
	}

	private void parseXmlFeed(Buffer buffer, Date maxDate, URL url, String feedId, Future<Void> future) {
		String xmlFeed = buffer.toString("UTF-8");
		StringReader xmlReader = new StringReader(xmlFeed);
		SyndFeedInput feedInput = new SyndFeedInput();
		try {
			SyndFeed feed = feedInput.build(xmlReader);
			JsonObject feedJson = FeedUtils.toJson(feed);
			log.info(feedJson);
			List<JsonObject> jsonEntries = FeedUtils.toJson(feed.getEntries(), maxDate);
			log.info("Insert " + jsonEntries.size() + " entries into Redis");
			if (jsonEntries.size() == 0) {
				future.complete();
				return;
			}
			vertx.eventBus().publish(feedId, new JsonArray(jsonEntries));
			redis.insertEntries(feedId, jsonEntries, handler -> {
				if (handler.failed()) {
					log.error("Insert failed", handler.cause());
					future.fail(handler.cause());
				} else {
					future.complete();
				}
			});
		} catch (FeedException fe) {
			log.error("Exception while reading feed : " + url.toString(), fe);
			future.fail(fe);
		}
	}

	private void getXML(URL url, Handler<HttpClientResponse> responseHandler) {
		client(url)
						.get(url.getPath(), responseHandler)
						.putHeader(HttpHeaders.ACCEPT, "application/xml")
						.end();
	}

	private HttpClient client(URL url) {
		HttpClient client = clients.get(url.getHost());
		if (client == null) {
			client = createClient(url);
			clients.put(url.getHost(), client);
		}
		return client;
	}

	private HttpClient createClient(URL url) {
		final HttpClientOptions options = new HttpClientOptions();
		options.setDefaultHost(url.getHost());
		return vertx.createHttpClient(options);
	}
}
