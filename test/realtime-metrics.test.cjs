/** @file Telemetría realtime: instrumentos cerrados, privacidad y recepción OTLP local con el SDK existente. */
'use strict';
const test                = require('node:test');
const assert              = require('node:assert/strict');
const { createServer }    = require('node:http');
const { realtimeMetrics } = require('../server/adapters/telemetry/realtime');

test('los contadores no aceptan dimensiones libres y un fallo de observación no altera el dato', /** @returns {void} El observador recibe sólo clasificaciones conocidas. */ () => {
  const records = [];
  const observe = realtimeMetrics({
    /** @returns {Object} API técnica aislada. */
    getMeter() {
      return {
        /** @param {string} name Nombre cerrado. @param {Object} options Unidad. @returns {Object} Instrumento de prueba. */
        createCounter(name, options) {
          return {
            /** @param {number} value Incremento. @param {Object} attributes Nunca presentes. @returns {void} Captura sólo información técnica. */
            add(value, attributes) {
              records.push({
                name,
                value,
                attributes,
                options,
              });
            },
          };
        },
      };
    },
  });
  observe('http-read', {
    userCode : 'PRIVATE',
    query    : 'PRIVATE',
  });
  observe('PRIVATE');
  observe('__proto__');
  observe('patch');
  assert.deepEqual(
    records.map(
      /** @param {Object} record Métrica. @returns {string} Instrumento elegido sin etiquetas dinámicas. */
      (record) => record.name,
    ),
    ['realtime.http.explicit.reads', 'realtime.delivery.patches'],
  );
  assert(
    records.every(
      /** @param {Object} record Métrica. @returns {boolean} Incremento unitario sin contenido. */
      (record) => record.value === 1 && record.attributes === undefined,
    ),
  );
  const broken = realtimeMetrics({
    /** @returns {Object} Exportador indisponible simulado. */
    getMeter() {
      return {
        /** @returns {never} La instrumentación es independiente del flujo funcional. */
        createCounter() {
          throw new Error('PRIVATE');
        },
      };
    },
  });
  assert.doesNotThrow(
    /** @returns {void} El error del instrumento no escapa. */
    () => broken('patch'),
  );
});

test('SDK desactivado conserva los contadores como no-op sin iniciar proveedores', /** @returns {void} El observador no carga ni arranca otro SDK. */ () => {
  const sdk = require.resolve('@opentelemetry/sdk-node');
  assert.equal(require.cache[sdk], undefined);
  const telemetry = require('../server/library/telemetry')
    .createTelemetry({ env: { OTEL_SDK_DISABLED: 'true' } })
    .start();
  assert.equal(telemetry.state, 'disabled');
  const observe = realtimeMetrics(require('@opentelemetry/api').metrics);
  observe('http-read');
  observe('patch');
  assert.equal(require.cache[sdk], undefined);
});

test(
  'OTLP real distingue bases HTTP, recuperación y patches sin exportar canarios',
  { timeout: 15000 },
  /** @returns {Promise<void>} Loopback efímero; sin Collector externo ni configuración de ambientes. */ async () => {
    const payloads = [];
    const server   = createServer(
      /** @param {Object} request OTLP HTTP. @param {Object} response Receptor local. @returns {void} Conserva bytes serializados realmente enviados por el SDK. */
      (request, response) => {
        const chunks = [];
        request.on(
          'data',
          /** @param {Buffer} chunk Fragmento protobuf. @returns {void} Captura acotada al test local. */
          (chunk) => chunks.push(chunk),
        );
        request.on(
          'end',
          /** @returns {void} Confirma la recepción del lote real. */
          () => {
            payloads.push({
              path : request.url,
              body : Buffer.concat(chunks),
            });
            response.writeHead(200, { 'Content-Type': 'application/x-protobuf' });
            response.end();
          },
        );
      },
    );
    await new Promise(
      /** @param {Function} resolve Continuación. @returns {void} Publica sólo en loopback con puerto efímero. */
      (resolve) => server.listen(0, '127.0.0.1', resolve),
    );
    const telemetry = require('../server/library/telemetry')
      .createTelemetry({
        serviceName      : 'realtime-test',
        serviceVersion   : '1.0.0',
        environment      : 'test',
        instrumentations : false,
        env              : {
          OTEL_EXPORTER_OTLP_ENDPOINT : `http://127.0.0.1:${server.address().port}`,
          OTEL_METRIC_EXPORT_INTERVAL : '60000',
          OTEL_EXPORTER_OTLP_TIMEOUT  : '1000',
        },
      })
      .start();
    try {
      assert.equal(telemetry.state, 'started');
      const observe = realtimeMetrics(require('@opentelemetry/api').metrics);
      for (const operation of [
        'http-read',
        'http-recovery',
        'window-read',
        'lease-renewal',
        'patch',
        'revoked',
        'error',
        'resync-required',
      ])
        observe(operation, {
          query    : 'CANARY_PRIVATE_REALTIME',
          userCode : 'CANARY_PRIVATE_REALTIME',
          token    : 'CANARY_PRIVATE_REALTIME',
        });
      observe('CANARY_PRIVATE_REALTIME');
      for (const operation of [
        'authorization',
        'receipt-read',
        'receipt-write',
        'producer-read',
        'bind',
      ]) {
        observe(operation, 12.5);
        observe(operation, { token: 'CANARY_PRIVATE_REALTIME' });
      }
      await telemetry.flush();
      const metrics = Buffer.concat(
        payloads
          .filter(
            /** @param {Object} payload Lote. @returns {boolean} Señal de métricas realmente recibida. */
            (payload) => payload.path === '/v1/metrics',
          )
          .map(
            /** @param {Object} payload Lote. @returns {Buffer} Bytes exportados, sin reconstruir desde el observador. */
            (payload) => payload.body,
          ),
      );
      for (const name of [
        'realtime.http.explicit.reads',
        'realtime.http.recovery.reads',
        'realtime.delivery.patches',
        'realtime.control.lease.renewals',
        'realtime.authorization.duration',
        'realtime.receipt.read.duration',
        'realtime.receipt.write.duration',
        'realtime.producer.read.duration',
        'realtime.bind.duration',
      ])
        assert(metrics.includes(Buffer.from(name)), name);
      assert(
        !Buffer.concat(
          payloads.map(
            /** @param {Object} payload Señal. @returns {Buffer} Datos recibidos de todas las señales. */
            (payload) => payload.body,
          ),
        ).includes(Buffer.from('CANARY_PRIVATE_REALTIME')),
      );
      server.closeAllConnections();
      await new Promise(
        /** @param {Function} resolve Continuación. @returns {void} Simula Collector caído sin tocar la aplicación. */
        (resolve) => server.close(resolve),
      );
      assert.doesNotThrow(
        /** @returns {void} El incremento local continúa con el Collector caído. */
        () => observe('patch'),
      );
    } finally {
      await telemetry.close({ deadlineAt: Date.now() + 2000 });
      server.closeAllConnections();
      server.close();
    }
  },
);
