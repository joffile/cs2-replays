const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { Readable } = require("stream");
const { pipeline } = require("stream/promises");
const { Worker, isMainThread, parentPort, workerData } = require("worker_threads");
const bz2 = require("unbzip2-stream");
const AdmZip = require("adm-zip");
const { parseTicks } = require("@laihoe/demoparser2");

const PORT = process.env.PORT || 3000;
const KEY = process.env.ACCESS_KEY || "";
const DATA = process.env.DATA_DIR || path.join(__dirname, "data");
const WORK = path.join(DATA, "work");
const PUBLIC = path.join(__dirname, "public");
const INDEX = path.join(DATA, "index.json");
const SAMPLE = 16;          // every 16th tick = 4 frames per second
const PER_CHUNK = 64 * 60;  // one chunk per minute of game time
const MAX_TICK = 400000;    // about 100 minutes
const PROPS = ["X", "Y", "team_num", "health", "name"];
const jobs = {};

function toRows(out) {
  if (typeof out === "string") out = JSON.parse(out);
  if (Array.isArray(out)) return out;
  const keys = Object.keys(out || {});
  const n = keys.length ? (out[keys[0]] || []).length : 0;
  const rows = [];
  for (let i = 0; i < n; i++) {
    const r = {};
    for (const k of keys) r[k] = out[k][i];
    rows.push(r);
  }
  return rows;
}

function buildChunks(rows) {
  const byTick = new Map();
  for (const r of rows) {
    if (!(r.health > 0)) continue;
    if (!byTick.has(r.tick)) byTick.set(r.tick, []);
    byTick.get(r.tick).push(r);
  }
  const names = [], chunks = {};
  for (const [t, ps] of [...byTick].sort((a, b) => a[0] - b[0])) {
    const f = [t];
    for (const p of ps) {
      let i = names.indexOf(p.name);
      if (i < 0) { names.push(p.name); i = names.length - 1; }
      f.push(i, Math.round(p.X), Math.round(p.Y), p.team_num);
    }
    const c = Math.floor(t / PER_CHUNK);
    (chunks[c] ||= []).push(f);
  }
  return { chunks, players: names };
}

function kindOf(file) {
  const head = Buffer.alloc(4);
  const fd = fs.openSync(file, "r");
  fs.readSync(fd, head, 0, 4, 0);
  fs.closeSync(fd);
  if (head.toString("latin1", 0, 3) === "BZh") return "bz2";
  if (head[0] === 0x50 && head[1] === 0x4b) return "zip";
  return "dem";
}

async function download(url, dest) {
  const res = await fetch(url, { redirect: "follow" });
  if (!res.ok) throw new Error("ссылка не открылась, код " + res.status);
  await pipeline(Readable.fromWeb(res.body), fs.createWriteStream(dest));
}

async function toDem(rawFile, demFile) {
  const kind = kindOf(rawFile);
  if (kind === "bz2") {
    await pipeline(fs.createReadStream(rawFile), bz2(), fs.createWriteStream(demFile));
  } else if (kind === "zip") {
    const entry = new AdmZip(rawFile).getEntries().find(e => /\.dem$/i.test(e.entryName));
    if (!entry) throw new Error("в архиве нет файла .dem");
    fs.writeFileSync(demFile, entry.getData());
  } else {
    fs.renameSync(rawFile, demFile);
  }
}

function titleFrom(url, title) {
  if (title) return title;
  try {
    return decodeURIComponent(new URL(url).pathname.split("/").pop())
      .replace(/\.(dem|zip|bz2)+$/i, "") || "Повтор";
  } catch {
    return "Повтор";
  }
}

function parseInWorker(demFile) {
  return new Promise((resolve, reject) => {
    const w = new Worker(__filename, { workerData: { dem: demFile } });
    w.once("message", m => (m.ok ? resolve(m.result) : reject(new Error(m.error))));
    w.once("error", reject);
    w.once("exit", code => reject(new Error("разбор остановился, код " + code)));
  });
}

async function runJob(id, url, title) {
  const job = jobs[id];
  const rawFile = path.join(WORK, id + ".raw");
  const demFile = path.join(WORK, id + ".dem");
  try {
    job.state = "скачиваю";
    await download(url, rawFile);
    job.state = "распаковываю";
    await toDem(rawFile, demFile);
    job.state = "разбираю демку, это может занять несколько минут";
    const { chunks, players } = await parseInWorker(demFile);
    if (!Object.keys(chunks).length) throw new Error("в демке нет данных об игроках");
    job.state = "сохраняю";
    const created = Date.now();
    fs.writeFileSync(path.join(DATA, id + ".json"), JSON.stringify({ title, players, chunks }));
    const index = fs.existsSync(INDEX) ? JSON.parse(fs.readFileSync(INDEX, "utf8")) : [];
    index.push({ id, title, created });
    fs.writeFileSync(INDEX, JSON.stringify(index));
    job.state = "готово";
  } catch (e) {
    job.state = "ошибка";
    job.error = e.message || String(e);
  } finally {
    fs.rmSync(rawFile, { force: true });
    fs.rmSync(demFile, { force: true });
  }
}

function json(res, code, obj) {
  res.writeHead(code, { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" });
  res.end(JSON.stringify(obj));
}

function sendRaw(res, code, body, type) {
  res.writeHead(code, { "Content-Type": type, "Cache-Control": "no-store" });
  res.end(body);
}

function readBody(req) {
  return new Promise((ok, bad) => {
    let d = "";
    req.on("data", c => { d += c; });
    req.on("end", () => { try { ok(JSON.parse(d || "{}")); } catch (e) { bad(e); } });
    req.on("error", bad);
  });
}

// Worker thread: parses the demo without blocking the web server.
if (!isMainThread) {
  try {
    const ticks = [];
    for (let t = 0; t < MAX_TICK; t += SAMPLE) ticks.push(t);
    const rows = toRows(parseTicks(workerData.dem, PROPS, ticks));
    if (!rows.length) throw new Error("парсер не вернул данных");
    parentPort.postMessage({ ok: true, result: buildChunks(rows) });
  } catch (e) {
    parentPort.postMessage({ ok: false, error: e.message || String(e) });
  }
}

// Main thread: the web server.
if (isMainThread) {
  fs.mkdirSync(WORK, { recursive: true });

  http.createServer(async (req, res) => {
    try {
      const u = new URL(req.url, "http://localhost");
      if (req.method === "GET" && u.pathname === "/") {
        return sendRaw(res, 200, fs.readFileSync(path.join(PUBLIC, "index.html")), "text/html; charset=utf-8");
      }
      if (!u.pathname.startsWith("/api/")) return json(res, 404, { error: "не найдено" });
      if (KEY && req.headers["x-key"] !== KEY) return json(res, 401, { error: "неверный ключ доступа" });

      if (req.method === "POST" && u.pathname === "/api/load") {
        const body = await readBody(req);
        const url = String(body.url || "").trim();
        if (!/^https?:\/\//i.test(url)) return json(res, 400, { error: "вставьте ссылку, которая начинается с https://" });
        const id = crypto.randomBytes(5).toString("hex");
        jobs[id] = { state: "в очереди" };
        runJob(id, url, titleFrom(url, String(body.title || "").trim()));
        return json(res, 200, { id });
      }

      let m = u.pathname.match(/^\/api\/status\/([a-f0-9]+)$/);
      if (m) return json(res, 200, jobs[m[1]] || { state: "неизвестно" });

      if (u.pathname === "/api/list") {
        const list = fs.existsSync(INDEX) ? JSON.parse(fs.readFileSync(INDEX, "utf8")) : [];
        return json(res, 200, list.reverse());
      }

      m = u.pathname.match(/^\/api\/replay\/([a-f0-9]+)$/);
      if (m) {
        const f = path.join(DATA, m[1] + ".json");
        if (!fs.existsSync(f)) return json(res, 404, { error: "нет такого повтора" });
        return sendRaw(res, 200, fs.readFileSync(f), "application/json; charset=utf-8");
      }

      json(res, 404, { error: "не найдено" });
    } catch (e) {
      json(res, 500, { error: e.message || String(e) });
    }
  }).listen(PORT, () => console.log("Сервер запущен на порту " + PORT));
}
