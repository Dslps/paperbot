// Cloudflare Worker: relays read-only Binance futures market data for paper_bot.py.
// Only /fapi/ GET requests are forwarded. No keys, no orders.
export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (request.method !== "GET" || !url.pathname.startsWith("/fapi/")) {
      return new Response("not found", { status: 404 });
    }
    const r = await fetch("https://fapi.binance.com" + url.pathname + url.search, {
      headers: { "User-Agent": "paperbot-relay" },
    });
    const headers = { "Content-Type": "application/json" };
    const ra = r.headers.get("Retry-After");
    if (ra) headers["Retry-After"] = ra;
    return new Response(r.body, { status: r.status, headers });
  },
};
