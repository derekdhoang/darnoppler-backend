/**
 * Darnoppler Tile Proxy
 * Serves HRRR radar forecast tiles from R2 to the frontend.
 * 
 * Endpoints:
 *   GET /latest              → returns { date, run, forecastHours } of freshest run
 *   GET /<date>/<run>z/...   → serves a tile from R2
 */

var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// src/index.js
var index_default = {
  async fetch(request, env, ctx) {
    if (request.method !== "GET") {
      return new Response("Method not allowed", { status: 405 });
    }
    const url = new URL(request.url);
    const path = url.pathname;
    if (path === "/latest" || path === "/latest/") {
      return await handleLatest(env);
    }
    const key = path.replace(/^\/+/, "");
    if (!key) {
      return new Response("Missing tile path", { status: 400 });
    }
    const object = await env.TILES.get(key);
    if (!object) {
      return new Response("Tile not found", { status: 404 });
    }
    const headers = new Headers();
    object.writeHttpMetadata(headers);
    headers.set("Cache-Control", "public, max-age=3600");
    headers.set("Access-Control-Allow-Origin", "*");
    headers.set("Access-Control-Allow-Methods", "GET");
    return new Response(object.body, { headers });
  }
};

async function handleLatest(env) {
  try {
    // Paginate through ALL date prefixes in case there are many
    let datePrefixes = [];
    let cursor = undefined;
    do {
      const listing = await env.TILES.list({
        delimiter: "/",
        limit: 1000,
        cursor,
      });
      datePrefixes.push(...(listing.delimitedPrefixes || []));
      cursor = listing.truncated ? listing.cursor : undefined;
    } while (cursor);

    if (datePrefixes.length === 0) {
      return jsonResponse({ error: "No data in bucket" }, 404);
    }

    const sortedDates = [...datePrefixes].sort().reverse();
    const latestDate = sortedDates[0].replace(/\/$/, "");

    // Paginate run prefixes too
    let runPrefixes = [];
    cursor = undefined;
    do {
      const listing = await env.TILES.list({
        prefix: `${latestDate}/`,
        delimiter: "/",
        limit: 1000,
        cursor,
      });
      runPrefixes.push(...(listing.delimitedPrefixes || []));
      cursor = listing.truncated ? listing.cursor : undefined;
    } while (cursor);

    if (runPrefixes.length === 0) {
      return jsonResponse({ error: "No runs for latest date" }, 404);
    }

    const sortedRuns = [...runPrefixes].sort().reverse();
    const latestRunPath = sortedRuns[0].replace(/\/$/, "");
    const runHour = latestRunPath.split("/")[1];

    const forecastHours = [];
    for (let fh = 0; fh <= 18; fh++) {
      const fhPrefix = `${latestRunPath}/f${String(fh).padStart(2, "0")}/`;
      const test = await env.TILES.list({ prefix: fhPrefix, limit: 1 });
      if (test.objects.length > 0) {
        forecastHours.push(fh);
      }
    }

    return jsonResponse({
      date: latestDate,
      run: runHour,
      forecastHours,
      availableRuns: sortedRuns.slice(0, 5).map((r) => r.replace(/\/$/, "")),
    });
  } catch (error) {
    return jsonResponse({ error: String(error) }, 500);
  }
}
__name(handleLatest, "handleLatest");

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET",
      "Cache-Control": "public, max-age=60",
    }
  });
}
__name(jsonResponse, "jsonResponse");

export {
  index_default as default
};
