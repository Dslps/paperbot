// Vercel function (Frankfurt): relays read-only Binance futures market data for paper_bot.py.
// /fapi/<path>?<query> is rewritten here as ?p=<path>&<query>. No keys, no orders.
export default async function handler(req, res) {
  const q = { ...req.query };
  const p = Array.isArray(q.p) ? q.p.join("/") : q.p || "";
  delete q.p;
  if (req.method !== "GET" || !/^v\d\/[A-Za-z]+$/.test(p)) {
    res.status(404).send("not found");
    return;
  }
  const qs = new URLSearchParams(q).toString();
  const r = await fetch(`https://fapi.binance.com/fapi/${p}${qs ? "?" + qs : ""}`, {
    headers: { "User-Agent": "paperbot-relay" },
  });
  const ra = r.headers.get("retry-after");
  if (ra) res.setHeader("Retry-After", ra);
  res.setHeader("Content-Type", "application/json");
  res.status(r.status).send(await r.text());
}
