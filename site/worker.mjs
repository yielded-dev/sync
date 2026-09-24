export default {
  fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/sync") {
      url.pathname = "/sync/";

      return Response.redirect(url, 308);
    }

    if (!url.pathname.startsWith("/sync/")) {
      return new Response("Not found", { status: 404 });
    }

    url.pathname = url.pathname.slice("/sync".length);

    return env.ASSETS.fetch(new Request(url, request));
  },
};
