package com.github.aesteve.vertx.nubes.examples.feeds.controllers.views;

import com.github.aesteve.vertx.nubes.annotations.Controller;
import com.github.aesteve.vertx.nubes.annotations.View;
import com.github.aesteve.vertx.nubes.annotations.routing.http.GET;

@Controller("/private/")
public class PrivateViews {

	@GET("manage")
	@View("private/manage.hbs")
	public void manage() {}

	@GET("news")
	@View("private/news.hbs")
	public void news() {}

}
