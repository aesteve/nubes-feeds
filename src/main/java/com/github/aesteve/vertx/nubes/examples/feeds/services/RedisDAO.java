package com.github.aesteve.vertx.nubes.examples.feeds.services;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import com.github.aesteve.vertx.nubes.examples.feeds.utils.rss.FeedUtils;
import com.github.aesteve.vertx.nubes.services.Service;
import com.github.aesteve.vertx.nubes.utils.async.AsyncUtils;

import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;
import io.vertx.redis.op.RangeLimitOptions;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisDAO implements Service {

	private static final Logger log = LoggerFactory.getLogger(RedisDAO.class);

	private RedisClient redis;
	private Vertx vertx;
	private RedisOptions options;

	public void getEntries(String feedHash, Date from, Date to, Handler<AsyncResult<JsonArray>> handler) {
		String fromStr;
		String toStr;
		if (from != null) {
			fromStr = Double.toString(Long.valueOf(from.getTime()).doubleValue());
		} else {
			fromStr = "-inf";
		}
		if (to != null) {
			toStr = Double.toString(Long.valueOf(from.getTime()).doubleValue());
		} else {
			toStr = "+inf";
		}
		redis.zrevrangebyscore(feedHash, toStr, fromStr, RangeLimitOptions.NONE, handler);
	}

	public void getMaxDate(String feedHash, Handler<Date> handler) {
		/*
		 * FIXME : this fails with a ClassCastException use it as soon as RedisClient is fixed
		 * RangeLimitOptions options = new RangeLimitOptions();
		 * options.setLimit(0, 1);
		 */
		redis.zrevrangebyscore(feedHash, "+inf", "-inf", RangeLimitOptions.NONE, result -> {
			if (result.failed()) {
				log.error("Fetch max date failed : ", result.cause());
				handler.handle(null);
			} else {
				JsonArray array = result.result();
				if (array.isEmpty()) {
					log.info("Fetch max date is null, array is empty for feedHash : " + feedHash);
					handler.handle(null);
					return;
				}
				JsonObject max = new JsonObject(array.getString(0));
				String published = max.getString("published");
				try {
					handler.handle(FeedUtils.getDate(published));
				} catch (ParseException pe) {
					log.error("Could not fetch max date : ", pe);
					handler.handle(null);
				}
			}
		});
	}

	public void insertEntries(String feedHash, List<JsonObject> entries, Handler<AsyncResult<Long>> handler) {
		Map<String, Double> members = new HashMap<String, Double>(entries.size());
		entries.forEach(entry -> {
			members.put(entry.toString(), entry.getDouble("score"));
		});
		redis.zaddMany(feedHash, members, handler);
	}

	@Override
	public void init(Vertx vertx, JsonObject config) {
		this.vertx = vertx;
		options = new RedisOptions();
		options.setHost("localhost");
		options.setPort(config.getInteger("mongo.port"));
	}

	@Override
	public void start(Future<Void> startFuture) {
		redis = RedisClient.create(vertx, options);
		startFuture.complete();
	}

	@Override
	public void stop(Future<Void> stopFuture) {
		redis.close(AsyncUtils.completeOrFail(stopFuture));
	}
}
