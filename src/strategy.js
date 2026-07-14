// Estrategia del bot: Hunt + Probability con parity search (FSM Hunt ↔ Target).
//
// El bot SOLO usa información visible de sus propios disparos (agua / impacto / hundido) y
// el tamaño de los barcos que aún no ha hundido — NUNCA conoce la posición real de la flota
// enemiga. Todo aquí son funciones puras sobre un objeto `estado`, sin Kafka ni Redis.
//
//   HUNT MODE  → genera un mapa de probabilidad (¿por cuántas colocaciones válidas de los
//                barcos restantes pasa cada casilla?) y dispara al máximo. Aplica parity
//                search para reducir ~50% los disparos sin perder cobertura.
//   TARGET MODE → al haber impactos sin hundir, solo considera colocaciones que cubren esos
//                impactos → persigue y deduce la orientación del barco hasta hundirlo.

export const TABLERO = 10;
export const FLOTA_INICIAL = [5, 4, 3, 3, 2]; // portaaviones, acorazado, crucero, submarino, destructor

const clave = (x, y) => `${x},${y}`;

export function crearEstado(size = TABLERO) {
  return { size, celdas: {}, barcos: [...FLOTA_INICIAL] };
}

// Registra el resultado de un disparo del bot en su tablero interno.
export function registrarDisparo(estado, x, y, result) {
  const k = clave(x, y);
  if (result === 'miss') estado.celdas[k] = 'miss';
  else if (result === 'hit') estado.celdas[k] = 'hit';
  else if (result === 'sunk') { estado.celdas[k] = 'hit'; hundir(estado, x, y); }
  return estado;
}

// Al hundir: marca el grupo contiguo de impactos como 'sunk' y quita ese tamaño de la lista
// de barcos restantes (tamaño del barco ≈ nº de impactos contiguos).
function hundir(estado, x, y) {
  const grupo = [];
  const visto = new Set();
  const pila = [[x, y]];
  while (pila.length) {
    const [cx, cy] = pila.pop();
    const k = clave(cx, cy);
    if (visto.has(k) || estado.celdas[k] !== 'hit') continue;
    visto.add(k);
    grupo.push([cx, cy]);
    pila.push([cx + 1, cy], [cx - 1, cy], [cx, cy + 1], [cx, cy - 1]);
  }
  for (const [cx, cy] of grupo) estado.celdas[clave(cx, cy)] = 'sunk';

  const tam = grupo.length;
  let idx = estado.barcos.indexOf(tam);
  if (idx === -1) {
    // Caso raro: dos barcos adyacentes se fusionaron en un grupo. Quita el mayor ≤ tam.
    idx = estado.barcos.reduce(
      (best, t, i) => (t <= tam && (best === -1 || t > estado.barcos[best]) ? i : best),
      -1,
    );
    if (idx === -1) idx = 0;
  }
  estado.barcos.splice(idx, 1);
}

// Mapa de probabilidad: cuenta cuántas colocaciones válidas de los barcos restantes pasan por
// cada casilla desconocida. requiereHit=true → solo colocaciones que cubren algún impacto (y
// se ponderan más las que cubren varios impactos alineados → deduce la orientación).
export function mapaProbabilidad(estado, requiereHit = false) {
  const { size, celdas, barcos } = estado;
  const prob = {};
  const dentro = (x, y) => x >= 0 && y >= 0 && x < size && y < size;
  const bloqueada = (x, y) => { const v = celdas[clave(x, y)]; return v === 'miss' || v === 'sunk'; };

  for (const tam of barcos) {
    for (const horizontal of [true, false]) {
      for (let y = 0; y < size; y++) {
        for (let x = 0; x < size; x++) {
          const cs = [];
          for (let i = 0; i < tam; i++) cs.push(horizontal ? [x + i, y] : [x, y + i]);
          if (!cs.every(([cx, cy]) => dentro(cx, cy))) continue;
          if (cs.some(([cx, cy]) => bloqueada(cx, cy))) continue;

          const hitsCubiertos = cs.filter(([cx, cy]) => celdas[clave(cx, cy)] === 'hit').length;
          if (requiereHit && hitsCubiertos === 0) continue;

          // En target, una colocación que cubre 2 impactos alineados vale mucho más → empuja
          // al bot a extender la línea en vez de probar perpendiculares.
          const peso = requiereHit ? 1 + hitsCubiertos * hitsCubiertos * 20 : 1;
          for (const [cx, cy] of cs) {
            if (celdas[clave(cx, cy)] === undefined) prob[clave(cx, cy)] = (prob[clave(cx, cy)] ?? 0) + peso;
          }
        }
      }
    }
  }
  return prob;
}

function elegirMax(prob, claves = Object.keys(prob)) {
  if (!claves.length) return null;
  const max = Math.max(...claves.map((k) => prob[k]));
  const mejores = claves.filter((k) => prob[k] === max);
  return mejores[Math.floor(Math.random() * mejores.length)].split(',').map(Number);
}

function primeraDesconocida(estado) {
  for (let y = 0; y < estado.size; y++)
    for (let x = 0; x < estado.size; x++)
      if (estado.celdas[clave(x, y)] === undefined) return [x, y];
  return null;
}

function modoHunt(estado) {
  const prob = mapaProbabilidad(estado, false);
  let claves = Object.keys(prob);
  if (!claves.length) return primeraDesconocida(estado);

  // Parity search: mientras el barco más pequeño mida N≥2, cualquier barco de tamaño ≥N cae
  // en al menos una casilla con (x+y)%N === offset. Filtramos al offset con más probabilidad
  // → ~mitad de disparos, misma cobertura garantizada.
  const minTam = Math.min(...estado.barcos);
  if (minTam >= 2) {
    const sumas = Array.from({ length: minTam }, () => 0);
    for (const k of claves) { const [x, y] = k.split(',').map(Number); sumas[(x + y) % minTam] += prob[k]; }
    const offset = sumas.indexOf(Math.max(...sumas));
    const filtradas = claves.filter((k) => { const [x, y] = k.split(',').map(Number); return (x + y) % minTam === offset; });
    if (filtradas.length) claves = filtradas;
  }
  return elegirMax(prob, claves);
}

function modoTarget(estado) {
  const prob = mapaProbabilidad(estado, true);
  const claves = Object.keys(prob);
  if (!claves.length) return modoHunt(estado); // por si el impacto quedó rodeado de agua/hundido
  return elegirMax(prob, claves);
}

// FSM: si hay impactos sin hundir → Target; si no → Hunt. Nunca devuelve una casilla ya disparada.
export function decidirDisparo(estado) {
  const hayImpactos = Object.values(estado.celdas).some((v) => v === 'hit');
  return hayImpactos ? modoTarget(estado) : modoHunt(estado);
}

// Reconstruye el estado del bot a partir del tablero PÚBLICO del enemigo (game:state):
// celdas agua/impacto/hundido y barcos restantes (deducidos de los grupos de celdas hundidas).
// Así el bot integra automáticamente los resultados de sus bombardeos (aparecen en el estado).
export function estadoDesdeTablero(size, cells) {
  const celdas = {};
  for (const [k, v] of Object.entries(cells ?? {})) {
    if (v === 'hit' || v === 'miss' || v === 'sunk') celdas[k] = v;
  }
  const barcos = [...FLOTA_INICIAL];
  const sunk = new Set(Object.entries(celdas).filter(([, v]) => v === 'sunk').map(([k]) => k));
  const visto = new Set();
  for (const inicio of sunk) {
    if (visto.has(inicio)) continue;
    let n = 0; const pila = [inicio];
    while (pila.length) {
      const k = pila.pop();
      if (visto.has(k) || !sunk.has(k)) continue;
      visto.add(k); n++;
      const [x, y] = k.split(',').map(Number);
      pila.push(`${x + 1},${y}`, `${x - 1},${y}`, `${x},${y + 1}`, `${x},${y - 1}`);
    }
    let idx = barcos.indexOf(n);
    if (idx === -1) idx = barcos.reduce((b, t, i) => (t <= n && (b === -1 || t > barcos[b]) ? i : b), -1);
    if (idx !== -1) barcos.splice(idx, 1);
  }
  return { size: size ?? TABLERO, celdas, barcos };
}
