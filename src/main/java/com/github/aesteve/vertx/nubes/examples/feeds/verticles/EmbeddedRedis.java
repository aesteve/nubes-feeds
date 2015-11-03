package com.github.aesteve.vertx.nubes.examples.feeds.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.io.IOException;

import com.github.aesteve.vertx.nubes.annotations.services.Verticle;

import redis.embedded.RedisServer;

@Verticle(inheritsConfig = true)
public class EmbeddedRedis extends AbstractVerticle {

	private RedisServer server;
	private JsonObject config;

	@Override
	public void init(Vertx vertx, Context context) {
		config = context.config();
	}

	@Override
	public void start(Future<Void> future) {
		try {
			server = new RedisServer(config.getInteger("redis.port"));
			server.start(); // seems to be blocking ?
			future.complete();
		} catch (IOException ioe) {
			future.fail(ioe);
		}
	}

	@Override
	public void stop(Future<Void> future) {
		if (server != null) {
			server.stop();
			server = null;
		}
		future.complete();
	}

}
