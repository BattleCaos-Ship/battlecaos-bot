// Contexto de correlationId por AsyncLocalStorage. Módulo HOJA (solo builtins de Node)
// para que tanto logger.js como observability.js lo importen sin ciclo de dependencias.
import { AsyncLocalStorage } from 'node:async_hooks';
import { randomUUID } from 'node:crypto';

const als = new AsyncLocalStorage();

// Ejecuta fn bajo el contexto de un correlationId (el del mensaje entrante, o uno nuevo).
export function conCorrelation(correlationId, fn) {
  return als.run({ correlationId: correlationId ?? randomUUID() }, fn);
}

// correlationId del procesamiento actual (o null fuera de contexto).
export function correlationActual() {
  return als.getStore()?.correlationId ?? null;
}
