import { describe, it, expect } from 'vitest';
import {
  crearEstado, registrarDisparo, decidirDisparo, mapaProbabilidad, estadoDesdeTablero, FLOTA_INICIAL,
} from '../strategy.js';

describe('crearEstado', () => {
  it('empieza con la flota completa y sin celdas conocidas', () => {
    const e = crearEstado();
    expect(e.barcos).toEqual(FLOTA_INICIAL);
    expect(Object.keys(e.celdas)).toHaveLength(0);
  });
});

describe('mapaProbabilidad (hunt)', () => {
  it('da más probabilidad al centro que a las esquinas', () => {
    const prob = mapaProbabilidad(crearEstado(), false);
    expect(prob['4,4']).toBeGreaterThan(prob['0,0']);
  });

  it('no asigna probabilidad a celdas ya disparadas', () => {
    const e = crearEstado();
    registrarDisparo(e, 5, 5, 'miss');
    const prob = mapaProbabilidad(e, false);
    expect(prob['5,5']).toBeUndefined();
  });

  it('el agua bloquea las colocaciones que la cruzan', () => {
    const e = crearEstado();
    registrarDisparo(e, 5, 5, 'miss');
    const prob = mapaProbabilidad(e, false);
    // (4,5) pierde probabilidad porque muchas colocaciones horizontales pasaban por (5,5)
    const limpio = mapaProbabilidad(crearEstado(), false);
    expect(prob['4,5']).toBeLessThan(limpio['4,5']);
  });
});

describe('decidirDisparo — HUNT', () => {
  it('devuelve una celda desconocida dentro del tablero', () => {
    const e = crearEstado();
    const [x, y] = decidirDisparo(e);
    expect(x).toBeGreaterThanOrEqual(0); expect(x).toBeLessThan(10);
    expect(y).toBeGreaterThanOrEqual(0); expect(y).toBeLessThan(10);
    expect(e.celdas[`${x},${y}`]).toBeUndefined();
  });

  it('aplica parity (tablero vacío → paridad determinista)', () => {
    const paridades = new Set();
    for (let i = 0; i < 15; i++) { const [x, y] = decidirDisparo(crearEstado()); paridades.add((x + y) % 2); }
    expect(paridades.size).toBe(1); // el barco menor es 2 → todos en la misma paridad
  });

  it('nunca dispara dos veces la misma celda', () => {
    const e = crearEstado();
    for (let i = 0; i < 50; i++) { const s = decidirDisparo(e); if (!s) break; registrarDisparo(e, s[0], s[1], 'miss'); }
    const disparos = Object.keys(e.celdas);
    expect(new Set(disparos).size).toBe(disparos.length);
  });
});

describe('decidirDisparo — TARGET', () => {
  it('tras un impacto dispara una celda adyacente', () => {
    const e = crearEstado();
    registrarDisparo(e, 5, 5, 'hit');
    const [x, y] = decidirDisparo(e);
    expect(Math.abs(x - 5) + Math.abs(y - 5)).toBe(1);
  });

  it('tras dos impactos alineados extiende la línea (no perpendicular)', () => {
    const e = crearEstado();
    registrarDisparo(e, 5, 5, 'hit');
    registrarDisparo(e, 6, 5, 'hit'); // línea horizontal
    const [, y] = decidirDisparo(e);
    expect(y).toBe(5); // continúa en la misma fila, extendiendo el barco
  });
});

describe('estadoDesdeTablero (reconstrucción desde game:state)', () => {
  it('copia agua/impacto/hundido y deduce barcos restantes de los grupos hundidos', () => {
    const cells = {
      '0,0': 'sunk', '1,0': 'sunk',   // un barco de tamaño 2 hundido
      '5,5': 'hit',                    // impacto activo
      '9,9': 'miss',
      '3,3': 'fog',                    // se ignora (no es agua/impacto/hundido)
    };
    const e = estadoDesdeTablero(10, cells);
    expect(e.celdas['0,0']).toBe('sunk');
    expect(e.celdas['5,5']).toBe('hit');
    expect(e.celdas['9,9']).toBe('miss');
    expect(e.celdas['3,3']).toBeUndefined();
    expect(e.barcos).toEqual([5, 4, 3, 3]); // se quitó el 2
  });

  it('el estado reconstruido entra en target si hay un impacto', () => {
    const e = estadoDesdeTablero(10, { '5,5': 'hit' });
    const [x, y] = decidirDisparo(e);
    expect(Math.abs(x - 5) + Math.abs(y - 5)).toBe(1); // dispara adyacente
  });
});

describe('hundimiento', () => {
  it('marca el barco como hundido, lo quita de la lista y vuelve a hunt', () => {
    const e = crearEstado();
    registrarDisparo(e, 0, 0, 'hit');
    registrarDisparo(e, 1, 0, 'sunk'); // hunde un barco de tamaño 2
    expect(e.celdas['0,0']).toBe('sunk');
    expect(e.celdas['1,0']).toBe('sunk');
    expect(e.barcos).toEqual([5, 4, 3, 3]); // se quitó el 2
    const s = decidirDisparo(e); // sin impactos activos → hunt
    expect(e.celdas[`${s[0]},${s[1]}`]).toBeUndefined();
  });
});
