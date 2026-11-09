/** @file Worker local durable con leases cercados y lotes de cien destinatarios. */
'use strict';
const { createReactiveWorker } = require('../../../library/work/reactive-worker');
/**
 * Construye un worker detenido; la composición controla arranque y cierre antes de MongoDB.
 * @param {Object} dependencies Repositorio, mensajería y logger técnico.
 * @returns {Object} start, close y tick para verificación determinista.
 */
function createNotificationsWorker({
  repository,
  messaging,
  changes,
  logger = {
    /**
     * Ofrece el punto de extensión error sin efectos por defecto.
     * @returns {void} Finaliza después de aplicar los efectos de la operación.
     */
    error() {},
  },
}) {
  let hasIndexesReady = false;
  /** Sólo fallos de disponibilidad explícitos pueden reintentarse automáticamente.
   * @param {Error} error Fallo recibido por la continuación.
   * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  function isTransient(error) {
    return (
      error?.retryable === true ||
      error?.statusCode === 503 ||
      ['RPC_TIMEOUT', 'RPC_UNCERTAIN', 'CONNECTION_LOST', 'NOT_AVAILABLE'].includes(error?.code) ||
      error?.hasErrorLabel?.('TransientTransactionError') ||
      ['MongoNetworkError', 'MongoServerSelectionError'].includes(error?.name)
    );
  }
  /** Procesa un lote con una sola propiedad; el ledger permite reanudar después de reiniciar.
   * @returns {Promise<boolean>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  async function tick() {
    if (!hasIndexesReady) {
      await repository.indexesReady();
      hasIndexesReady = true;
    }
    const job = await repository.claim();
    if (!job) return false;
    try {
      const batch = await repository.batch(job);
      if (!batch.length) return false;
      for (const row of batch) {
        const renewed = await repository.renew(job);
        if (!renewed.matchedCount) break;
        try {
          const response = await messaging.request('service-security', 'workspace-users-get-list', {
            workspace : job.workspace,
            userCodes : [row.userCode],
          });
          if (response.statusCode !== 200 || !Array.isArray(response.data?.items)) {
            const error = new Error('DEPENDENCY_UNAVAILABLE');
            error.statusCode = response.statusCode || 503;
            throw error;
          }
          await repository.persistRecipient(
            job,
            row,
            response.data.items.some(
              /**
               * Comprueba si el elemento satisface la condición.
               * @param {*} item Valor de item utilizado por esta operación.
               * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
               */
              (item) => item.userCode === row.userCode,
            ),
          );
        } catch (error) {
          if (error.descriptionCode === 'NOTIFICATION_LEASE_LOST') break;
          await repository.failRecipient(job, row, isTransient(error));
        }
      }
      return true;
    } finally {
      await repository.release(job);
    }
  }
  return {
    ...createReactiveWorker({
      source    : changes,
      nextAt    : repository.nextAt,
      tick,
      logger,
      component : 'notifications',
    }),
    tick,
  };
}

module.exports = { createNotificationsWorker };
