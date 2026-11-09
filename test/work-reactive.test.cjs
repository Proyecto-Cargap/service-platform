/** @file Verifica cancelación, aislamiento y reinicio del trabajador reactivo sin consultas periódicas. */
'use strict';
const test                     = require('node:test');
const assert                   = require('node:assert/strict');
const { setImmediate: turn }   = require('node:timers/promises');
const { createReactiveWorker } = require('../server/library/work/reactive-worker');
test('el cierre espera el efecto propio, cancela el stream y permite un reinicio explícito', /** @returns {Promise<void>} Fuente y efecto controlados sin MongoDB. */ async () => {
  let starts = 0,
    closes   = 0,
    reads    = 0,
    release,
    options,
    due      = 0,
    changed;
  const source = {
    ready : false,
    /** @param {Function} callback Señal de cambios. @returns {void} Confirma una conexión real simulada. */
    start(callback) {
      starts++;
      changed = callback;
      this.ready = true;
      callback();
    },
    /** @returns {Promise<void>} Cancela únicamente la fuente propia. */
    async close() {
      closes++;
      this.ready = false;
      changed();
    },
  };
  const worker = createReactiveWorker({
    source,
    /** @returns {Promise<number|undefined>} No tiene trabajo después del efecto. */
    async nextAt() {
      reads++;
      return due;
    },
    /** @param {Object} input Señal de cierre. @returns {Promise<void>} Efecto retenido para verificar el cerco. */
    async tick(input) {
      options = input;
      await new Promise(
        /** @param {Function} resolve Finalización controlada. @returns {void} Conserva la entrega. */ (
          resolve,
        ) => {
          release = resolve;
        },
      );
      due = undefined;
    },
    logger    : {
      /** @returns {void} El recorrido no incluye errores. */ error() {
        assert.fail('Fallo inesperado.');
      },
    },
    component : 'test',
  });
  worker.start();
  worker.start();
  await turn();
  assert.equal(starts, 1);
  assert.equal(options.shouldStop(), false);
  let closed   = false;
  const ending = worker.close().then(
    /** @returns {void} Confirma el cierre después del efecto. */ () => {
      closed = true;
    },
  );
  assert.equal(options.shouldStop(), true);
  assert.equal(closed, false);
  release();
  await ending;
  assert.equal(closes, 1);
  worker.start();
  await turn();
  assert.equal(starts, 2);
  const before = reads;
  await turn();
  await turn();
  assert.equal(reads, before);
  await worker.close();
  assert.equal(closes, 2);
});
test('una fuente indisponible suspende consultas y reanuda por confirmación del stream', /** @returns {Promise<void>} Verifica que no existe fallback a sondeo. */ async () => {
  let changed,
    reads   = 0,
    effects = 0,
    due;
  const source = {
    ready : false,
    /** @param {Function} notify Señal de disponibilidad. @returns {void} Conserva la conexión todavía pendiente. */
    start(notify) {
      changed = notify;
    },
    /** @returns {Promise<void>} Libera la fuente. */ async close() {
      this.ready = false;
      changed();
    },
  };
  const worker = createReactiveWorker({
    source,
    component : 'test',
    /** @returns {Promise<number|undefined>} Lectura sólo con continuidad acreditada. */ async nextAt() {
      reads++;
      return due;
    },
    /** @returns {Promise<void>} Consume una transición. */ async tick() {
      effects++;
      due = undefined;
    },
    logger    : {
      /** @returns {void} La indisponibilidad de la fuente no se convierte en fallo de consulta. */ error() {
        assert.fail('Fallo inesperado.');
      },
    },
  });
  worker.start();
  await turn();
  assert.equal(reads, 0);
  source.ready = true;
  changed();
  await turn();
  assert.equal(reads, 1);
  source.ready = false;
  changed();
  due = 0;
  const before = reads;
  await turn();
  await turn();
  assert.equal(reads, before);
  assert.equal(effects, 0);
  source.ready = true;
  changed();
  await turn();
  assert.equal(effects, 1);
  await worker.close();
});
