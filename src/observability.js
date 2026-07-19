// Observabilidad de un servicio INTERNO (sin ingress de negocio): levanta un servidor
// HTTP mínimo (http nativo, sin Express) con /health y /metrics. Es ADITIVO — corre en
// paralelo al consumer de Kafka y no toca la lógica de dominio.
//   • /metrics → formato Prometheus (prom-client): default metrics + contadores propios.
//   • /health  → JSON con estado de Redis primario/respaldo y Mongo; sirve además como
//     liveness/readiness probe para que Azure Container Apps reemplace réplicas muertas.
import http from 'node:http';
import client from 'prom-client';
import { log } from './logger.js';
// El contexto de correlationId vive en correlation.js (módulo hoja) para no crear
// un ciclo logger.js ↔ observability.js. Se re-exporta aquí por comodidad (los
// index.js ya importan conCorrelation/correlationActual desde observability.js).
import { conCorrelation, correlationActual } from './correlation.js';
export { conCorrelation, correlationActual };

const SERVICE = process.env.SERVICE_NAME ?? 'servicio';
// El prefix va DENTRO de nombres de metrica Prometheus, que solo admiten [a-zA-Z0-9_:]. Un
// SERVICE_NAME con guion (p.ej. "voice-channel") producia "voice-channel_..." -> prom-client
// lanzaba "Invalid metric name" y el proceso NO arrancaba. Se sanea a "_" para que sea valido.
const METRIC_PREFIX = `${SERVICE.replace(/[^a-zA-Z0-9_]/g, "_")}_`;

export const register = new client.Registry();
client.collectDefaultMetrics({ register, prefix: METRIC_PREFIX }); // CPU, memoria, event-loop lag

// ── Métricas de dominio comunes a todos los servicios internos ────────────────
export const eventsProcessed = new client.Counter({
  name: 'events_processed_total',
  help: 'Mensajes de Kafka procesados, por tipo y resultado',
  labelNames: ['type', 'status'], // status: ok | error
  registers: [register],
});

export const eventLatency = new client.Histogram({
  name: 'event_processing_seconds',
  help: 'Latencia de procesamiento de un mensaje de Kafka',
  labelNames: ['type'],
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2],
  registers: [register],
});

// Salud de la redundancia de datos: 1 = nodo arriba, 0 = caído.
export const redisNodeUp = new client.Gauge({
  name: 'redis_node_up',
  help: '1 si el nodo Redis responde a PING (node=primary|secondary)',
  labelNames: ['node'],
  registers: [register],
});
export const mongoUp = new client.Gauge({
  name: 'mongo_up',
  help: '1 si Mongo responde (0 si no aplica o está caído)',
  registers: [register],
});

// Salud del CONSUMER de Kafka: 1 = unido al grupo y consumiendo, 0 = caído/reconectando.
// Sin esto, un consumer colgado pasa desapercibido (el proceso sigue vivo y /health daba
// 200 solo por Redis). Ver hallazgo #1 de la auditoría.
export const kafkaConsumerUp = new client.Gauge({
  name: 'kafka_consumer_up',
  help: '1 si el consumer de Kafka está unido al grupo y activo',
  registers: [register],
});
let _consumerTracked  = false; // ¿se registró un consumer para vigilar?
let _consumerHealthy  = false; // estado actual
let _consumerJoinedEver = false; // ¿llegó a unirse alguna vez? (evita matar durante el arranque)

// Vigila los eventos del consumer de KafkaJS y refleja su estado en el gauge + /health.
// Llamar DESPUÉS de crear el consumer, ANTES de consumer.run().
export function trackConsumer(consumer) {
  _consumerTracked = true;
  const arriba = () => { _consumerHealthy = true;  _consumerJoinedEver = true; kafkaConsumerUp.set(1); };
  const abajo  = () => { _consumerHealthy = false; kafkaConsumerUp.set(0); };
  consumer.on(consumer.events.GROUP_JOIN, arriba);
  consumer.on(consumer.events.CONNECT,    arriba);
  consumer.on(consumer.events.CRASH,      abajo);
  consumer.on(consumer.events.STOP,       abajo);
  consumer.on(consumer.events.DISCONNECT, abajo);
  kafkaConsumerUp.set(0);
}

// Consultas lentas: longitud del SLOWLOG de Redis (1 de las 6 métricas de escalabilidad).
export const redisSlowlogLen = new client.Gauge({
  name: 'redis_slowlog_len',
  help: 'Entradas en el SLOWLOG de Redis (consultas por encima del umbral)',
  registers: [register],
});

// Activa el logging de consultas lentas en Redis (SLOWLOG) y en Mongo (profiler), y
// refresca redis_slowlog_len periódicamente. Idempotente. Lo llama UN solo servicio
// (observability) porque la config de Redis/Mongo es global, no por conexión.
export async function configurarSlowQueries(redis, { redisSlowMs = 10, mongoSlowMs = 100, mongoDb = null } = {}) {
  try {
    // slowlog-log-slower-than se expresa en MICROsegundos.
    await redis.config?.('SET', 'slowlog-log-slower-than', String(redisSlowMs * 1000));
    log.info(`redis SLOWLOG activado (umbral ${redisSlowMs}ms)`);
  } catch (e) { log.warn(`no se pudo activar el SLOWLOG de Redis — ${e.message}`); }

  if (mongoDb?.command) {
    try {
      await mongoDb.command({ profile: 1, slowms: mongoSlowMs }); // nivel 1 = solo operaciones lentas
      log.info(`mongo profiler activado (umbral ${mongoSlowMs}ms)`);
    } catch (e) { log.warn(`no se pudo activar el profiler de Mongo — ${e.message}`); }
  }

  const t = setInterval(async () => {
    try { const len = await redis.slowlog?.('LEN'); if (typeof len === 'number') redisSlowlogLen.set(len); }
    catch { /* nodo ocupado/caído: se reintenta al próximo tick */ }
  }, 10000);
  t.unref?.();
}

// Envuelve un handler de mensaje para instrumentarlo (latencia + contador ok/error).
// Uso: consumer.run({ eachMessage: instrument(async ({...}) => {...}) })... o por tipo.
export function instrumentar(type, fn) {
  return async (...args) => {
    const fin = eventLatency.startTimer({ type });
    try {
      const r = await fn(...args);
      eventsProcessed.inc({ type, status: 'ok' });
      return r;
    } catch (err) {
      eventsProcessed.inc({ type, status: 'error' });
      throw err;
    } finally {
      fin();
    }
  };
}

// Chequeo de salud on-demand: pinguea Redis (ambos nodos) y Mongo, y refresca los gauges.
async function chequear({ redis, mongo }) {
  const rh = redis?.health ? await redis.health() : { primary: null, secondary: null };
  redisNodeUp.set({ node: 'primary' },   rh.primary   ? 1 : 0);
  redisNodeUp.set({ node: 'secondary' }, rh.secondary ? 1 : 0);

  let mongoOk = null;
  if (mongo?.command) {
    try { await mongo.command({ ping: 1 }); mongoOk = true; } catch { mongoOk = false; }
  }
  mongoUp.set(mongoOk ? 1 : 0);

  // El consumer está "muerto" solo si YA se había unido y ahora está caído (así el arranque
  // no dispara falsos 503 antes del primer GROUP_JOIN). Cuando lo está, /health devuelve 503
  // → la liveness probe reinicia la réplica (que era justo lo que faltaba en el hallazgo #1).
  const consumerMuerto = _consumerTracked && _consumerJoinedEver && !_consumerHealthy;

  // "ok" mientras haya AL MENOS un nodo de Redis vivo Y el consumer no esté muerto.
  const redisOk = rh.primary || rh.secondary;
  const status  = (redisOk === false || consumerMuerto) ? 'error' : 'ok';
  return {
    service: SERVICE,
    status,
    redis: { primary: rh.primary, secondary: rh.secondary },
    mongo: mongoOk,
    kafkaConsumer: _consumerTracked ? (_consumerHealthy ? 'up' : (_consumerJoinedEver ? 'down' : 'starting')) : 'n/a',
    uptime: process.uptime(),
    timestamp: Date.now(),
  };
}

// Arranca el servidor de observabilidad. `deps`:
//   redis, mongo?  → para /health.
//   traceLookup?   → async (cid) => [eventos] para GET /trace/:cid (trazado distribuido
//                    por correlationId; lo provee el servicio observability).
export function startObservability({ port, redis, mongo = null, traceLookup = null }) {
  const server = http.createServer(async (req, res) => {
    try {
      if (req.url === '/metrics') {
        res.setHeader('Content-Type', register.contentType);
        res.end(await register.metrics());
        return;
      }
      if (req.url === '/health' || req.url === '/') {
        const h = await chequear({ redis, mongo });
        res.setHeader('Content-Type', 'application/json');
        res.statusCode = h.status === 'ok' ? 200 : 503;
        res.end(JSON.stringify(h));
        return;
      }
      // Trazado distribuido: la línea de tiempo de un evento a través de TODOS los
      // servicios, reconstruida por su correlationId. Alternativa libre a un APM.
      if (traceLookup && req.url.startsWith('/trace/')) {
        const cid = decodeURIComponent(req.url.slice('/trace/'.length));
        const eventos = await traceLookup(cid);
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ correlationId: cid, spans: eventos }));
        return;
      }
      res.statusCode = 404;
      res.end();
    } catch (err) {
      res.statusCode = 500;
      res.end(JSON.stringify({ error: err.message }));
    }
  });
  server.listen(port, () => log.info(`observabilidad :${port} — /health · /metrics${traceLookup ? ' · /trace/:cid' : ''}`));
  return server;
}
