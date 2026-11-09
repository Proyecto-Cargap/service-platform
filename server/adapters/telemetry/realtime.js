/** @file Contadores realtime de nombres estáticos sobre el proveedor OTel existente; sin datos ni etiquetas de usuario. */
'use strict';
const names     = Object.freeze({
  'http-read'       : 'realtime.http.explicit.reads',
  'http-recovery'   : 'realtime.http.recovery.reads',
  'window-read'     : 'realtime.producer.window.reads',
  'lease-renewal'   : 'realtime.control.lease.renewals',
  patch             : 'realtime.delivery.patches',
  revoked           : 'realtime.delivery.revocations',
  error             : 'realtime.delivery.errors',
  'resync-required' : 'realtime.delivery.recoveries',
});
const durations = Object.freeze({
  authorization   : 'realtime.authorization.duration',
  'receipt-read'  : 'realtime.receipt.read.duration',
  'receipt-write' : 'realtime.receipt.write.duration',
  'producer-read' : 'realtime.producer.read.duration',
  bind            : 'realtime.bind.duration',
});
/** @param {Object} metrics API del proveedor global ya instalado. @returns {Function} Observador sin SDK, timers ni exportadores propios. */
function realtimeMetrics(metrics) {
  const meter      = metrics.getMeter('realtime', '1.0.0');
  const counters   = new Map();
  const histograms = new Map();
  /** @param {string} operation Clasificación del motor. @returns {void} Omite valores desconocidos y nunca exporta atributos del evento. */
  return (operation, duration) => {
    try {
      if (Object.hasOwn(durations, operation)) {
        if (typeof duration !== 'number' || !Number.isFinite(duration) || duration < 0) return;
        let histogram = histograms.get(operation);
        if (!histogram) {
          histogram = meter.createHistogram(durations[operation], { unit: 'ms' });
          histograms.set(operation, histogram);
        }
        histogram.record(duration);
        return;
      }
      if (!Object.hasOwn(names, operation)) return;
      let counter = counters.get(operation);
      if (!counter) {
        counter = meter.createCounter(names[operation], { unit: '{operation}' });
        counters.set(operation, counter);
      }
      counter.add(1);
    } catch {
      /* La instrumentación no determina disponibilidad funcional. */
    }
  };
}
module.exports = { realtimeMetrics };
