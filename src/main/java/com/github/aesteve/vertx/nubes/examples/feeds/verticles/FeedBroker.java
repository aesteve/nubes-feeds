package com.github.aesteve.vertx.nubes.examples.feeds.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.mongo.MongoClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.aesteve.vertx.nubes.annotations.services.PeriodicTask;
import com.github.aesteve.vertx.nubes.annotations.services.Service;
import com.github.aesteve.vertx.nubes.annotations.services.Verticle;
import com.github.aesteve.vertx.nubes.examples.feeds.services.RedisDAO;
import com.github.aesteve.vertx.nubes.examples.feeds.utils.rss.FeedReader;
import com.github.aesteve.vertx.nubes.utils.async.MultipleFutures;

@Verticle
public class FeedBroker extends AbstractVerticle {

	private final static long POLL_PERIOD = 10000l;
	private final static Logger log = LoggerFactory.getLogger(FeedBroker.class);

	@Service("mongo")
	private MongoClient mongo;

	@Service("redis")
	private RedisDAO redis;

	private Map<String, HttpClient> clients;

	@Override
	public void init(Vertx vertx, Context context) {
		super.init(vertx, context);
		clients = new HashMap<String, HttpClient>();
	}

	@Override
	public void stop(Future<Void> future) {
		clients.forEach((url, client) -> {
			client.close();
		});
		future.complete();
	}

	@PeriodicTask(POLL_PERIOD)
	private void fetchFeeds() {
		JsonObject crit = new JsonObject();
		JsonObject gt0 = new JsonObject();
		gt0.put("$gt", 0);
		crit.put("subscriber_count", gt0);
		mongo.find("feeds", crit, result -> {
			if (result.failed()) {
				log.error("Could not retrieve feed list from Mongo", result.cause());
			} else {
				this.readFeeds(result.result());
			}
		});
	}

	private void readFeeds(List<JsonObject> feeds) {
		new MultipleFutures<Void>(feeds, feed -> {
			return new FeedReader(vertx, redis, clients, feed)::readFeed;
		}).start();
	}

}
