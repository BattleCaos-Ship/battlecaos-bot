import 'dotenv/config';
import { producer, createConsumer } from './kafka.js';
import { log } from './logger.js';
import { startObservability, conCorrelation, correlationActual, instrumentar , trackConsumer} from './observability.js';
import { estadoDesdeTablero, decidirDisparo } from './strategy.js';

// El bot no usa Redis; /health solo reporta que el proceso vive (redis=null → ok).
startObservability({ port: process.env.OBS_PORT ?? 9100, redis: null });

const BOT_ID = 'bot';
const BOT_DELAY_MS = parseInt(process.env.BOT_DELAY_MS ?? '800');

// Costos de poder (duplicados aquí porque el bot es un servicio aparte; deben coincidir con game).
const COSTOS = { escudo: 1, bombardeo: 2, tormenta: 3 };

// Para no actuar dos veces en el mismo turno (game:state se difunde varias veces por turno).
const turnoActuado = {}; // { [codigo]: numeroTurno }

await producer.connect();
log.info('kafka producer conectado');

// El bot se alimenta del ESTADO PÚBLICO del juego (gw.broadcast): de ahí saca su energía, su
// escudo, y el tablero enemigo. Así decide disparos Y poderes con información autoritativa.
const consumer = createConsumer('bot-group');
  trackConsumer(consumer); // salud del consumer -> kafka_consumer_up + /health
await consumer.connect();
await consumer.subscribe({ topics: ['gw.broadcast'], fromBeginning: false });
log.info('suscrito a gw.broadcast');

await consumer.run({
  eachMessage: async ({ message }) => {
    const msg = JSON.parse(message.value.toString());
    if (msg.event !== 'game:state') return;
    await conCorrelation(msg.correlationId, async () => {
      try {
        await instrumentar('game:state', async () => procesarEstado(msg.payload))();
      } catch (err) {
        log.error(`mensaje no procesado — ${err.message} [cid=${correlationActual()}]`);
      }
    });
  },
});

function procesarEstado(s) {
  const codigo = s?.codigo;
  if (!codigo) return;
  const bot = s.jugadores?.find((j) => j.id === BOT_ID);
  if (!bot) return; // no es una partida con bot

  if (s.fase === 'FIN') { delete turnoActuado[codigo]; return; }
  if (s.fase !== 'TURNOS') return;                 // el bot no actúa en COLOCACION/SALVA
  if (s.turno?.jugadorActual !== BOT_ID) return;   // no es su turno

  const numeroTurno = s.turno?.numeroTurno;
  if (turnoActuado[codigo] === numeroTurno) return; // ya actuó este turno
  turnoActuado[codigo] = numeroTurno;

  // Retraso artificial para que se sienta como un oponente humano.
  setTimeout(() => actuar(codigo, s, bot).catch((err) => log.error(`error turno bot ${codigo} —`, err.message)), BOT_DELAY_MS);
}

async function actuar(codigo, s, bot) {
  const equipo  = bot.equipo;
  const enemigo = equipo === 'A' ? 'B' : 'A';
  const tablero = s.tableroPublico?.[enemigo] ?? { size: 10, cells: {} };
  const estado  = estadoDesdeTablero(tablero.size, tablero.cells);

  let energia = s.energia?.[equipo] ?? 0;
  const escudoActivo   = !!s.escudos?.[equipo];
  const tormentaUsada  = !!s.tormentaUsada?.[BOT_ID];
  const enTarget = Object.values(estado.celdas).some((v) => v === 'hit');

  const centro = decidirDisparo(estado); // mejor objetivo (hunt/target)
  if (!centro) return;

  // ── Decidir poderes con el presupuesto de energía ──────────────────────────
  const poderes = [];
  // Tormenta: en persecución, gasta energía para un turno extra y rematar el barco.
  if (enTarget && !tormentaUsada && energia >= COSTOS.tormenta) {
    poderes.push({ powerType: 'tormenta', target: null });
    energia -= COSTOS.tormenta;
  }
  // Bombardeo: arrasa el área 3×3 del mejor objetivo (acelera hundir / explora).
  let disparo = centro;
  if (energia >= COSTOS.bombardeo) {
    poderes.push({ powerType: 'bombardeo', target: { x: centro[0], y: centro[1] } });
    energia -= COSTOS.bombardeo;
    // El disparo normal (que avanza el turno) va FUERA del área bombardeada, para no gastarlo.
    const copia = { size: estado.size, celdas: { ...estado.celdas }, barcos: [...estado.barcos] };
    for (let dx = -1; dx <= 1; dx++) for (let dy = -1; dy <= 1; dy++) {
      const k = `${centro[0] + dx},${centro[1] + dy}`;
      if (copia.celdas[k] === undefined) copia.celdas[k] = 'miss';
    }
    disparo = decidirDisparo(copia) ?? centro;
  }
  // Escudo: con lo que quede, protege la flota si no hay uno activo.
  if (!escudoActivo && energia >= COSTOS.escudo) {
    poderes.push({ powerType: 'escudo', target: null });
    energia -= COSTOS.escudo;
  }

  // ── Emitir: primero los poderes, luego el disparo (que avanza el turno) ─────
  for (const p of poderes) await emitir('BotPower', codigo, { codigo, playerId: BOT_ID, powerType: p.powerType, target: p.target });
  await emitir('BotDecision', codigo, { codigo, playerId: BOT_ID, x: disparo[0], y: disparo[1] });

  const detalle = poderes.length ? ` (+ ${poderes.map((p) => p.powerType).join(', ')})` : '';
  log.info(`sala ${codigo} — bot dispara (${disparo[0]},${disparo[1]})${detalle}`);
}

async function emitir(type, codigo, data) {
  await producer.send({
    topic:    'evt.bot',
    messages: [{ key: codigo, value: JSON.stringify({ type, source: 'bot', timestamp: Date.now(), version: 1, correlationId: correlationActual(), data }) }],
  });
}
