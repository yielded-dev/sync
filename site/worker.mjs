const SYNC_PREFIX = "/sync";
const NOT_FOUND_DOCUMENTS = new Set(["/404", "/404/", "/404.html", "/404/index.html"]);

// Asset lookup strips /sync, so html_handling redirects are rooted at /.
// Put the prefix back before the browser leaves this Worker.
const restoreSyncPrefix = (location, publicUrl) => {
  const target = new URL(location, publicUrl);

  if (target.origin !== publicUrl.origin) {
    return location;
  }

  if (target.pathname === SYNC_PREFIX || target.pathname.startsWith(`${SYNC_PREFIX}/`)) {
    return target.toString();
  }

  target.pathname = `${SYNC_PREFIX}${target.pathname.startsWith("/") ? target.pathname : `/${target.pathname}`}`;

  return target.toString();
};

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === SYNC_PREFIX) {
      url.pathname = `${SYNC_PREFIX}/`;

      return Response.redirect(url, 308);
    }

    if (!url.pathname.startsWith(`${SYNC_PREFIX}/`)) {
      return new Response("Not found", { status: 404 });
    }

    url.pathname = url.pathname.slice(SYNC_PREFIX.length);

    if (NOT_FOUND_DOCUMENTS.has(url.pathname)) {
      const pageUrl = new URL(url);

      pageUrl.pathname = "/404";
      const page = await env.ASSETS.fetch(new Request(pageUrl, request));

      return new Response(page.body, {
        status: 404,
        statusText: "Not Found",
        headers: page.headers,
      });
    }

    const asset = await env.ASSETS.fetch(new Request(url, request));
    const location = asset.headers.get("Location");

    if (location !== null && asset.status >= 300 && asset.status < 400) {
      const headers = new Headers(asset.headers);

      headers.set("Location", restoreSyncPrefix(location, new URL(request.url)));

      return new Response(asset.body, {
        status: asset.status,
        statusText: asset.statusText,
        headers,
      });
    }

    return asset;
  },
};
