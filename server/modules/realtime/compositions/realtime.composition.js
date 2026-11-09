/** @file Ventanas estándar de Platform: registro cerrado, recibos compartidos y consultas del propietario. */
'use strict';
const { RealtimeWindows }  = require('../../../library/realtime/windows');
const { ApplicationError } = require('../../../shared/application-error');
let windows;
let repositories;
/** @param {Object} dependencies Transporte y Mongo de Platform. @returns {Promise<Object>} Lectura privada del API Gateway. */
async function createRealtimeWindows({ messaging, connection, platformRead }) {
  const receipts = require('../repositories/read-receipts').readReceipts(connection);
  await receipts.verify();
  repositories = require('../services/repository-read.service').repositoryReads({
    receipts,
    messaging,
    clock      : Date.now,
    identifier : require('node:crypto').randomUUID,
    observe    : require('../../../adapters/telemetry/realtime').realtimeMetrics(
      require('@opentelemetry/api').metrics,
    ),
    hash       : require('../../../library/realtime/window-delta').contentCursor,
  });
  windows?.close();
  windows = new RealtimeWindows({
    receipts,
    maxSubscriptions : 128,
    observe          : require('../../../adapters/telemetry/realtime').realtimeMetrics(
      require('@opentelemetry/api').metrics,
    ),
    topics           : {
      ...require('../services/platform-window.service').platformWindows({
        messaging,
        read : platformRead,
      }),
      ...require('../services/security-window.service').securityWindows({ messaging }),
      ...require('../services/supplier-window.service').supplierWindows({ messaging }),
      ...require('../services/product-window.service').productWindows({ messaging }),
      ...require('../services/customer-window.service').customerWindows({ messaging }),
      ...require('../services/directory-window.service').directoryWindows({ messaging }),
      ...require('../services/billing-window.service').billingWindows({ messaging }),
      ...require('../services/intelligence-window.service').intelligenceWindows({ messaging }),
      ...require('../services/connect-window.service').connectWindows({ messaging }),
      ...require('../services/mailbox-window.service').mailboxWindows({ messaging }),
      ...require('../services/documents-resource-window.service').documentsResourceWindows({
        messaging,
      }),
      'documents.view' : require('../services/documents-window.service').documentsWindows({
        messaging,
      }),
    },
  });
  return {
    /** @param {Object} message Página acreditada por el API Gateway. @returns {Promise<Object>} Resultado canónico del manifiesto y su correlación. */
    async repositoryRead(message) {
      if (message.originService !== 'service-apigateway')
        throw new ApplicationError(403, 'FORBIDDEN');
      return {
        statusCode      : 200,
        descriptionCode : 'OK',
        data            : await repositories.read(message.data, { context: messaging.getContext() }),
      };
    },
    /** @param {Object} message Lectura delegada acreditada por Gateway. @returns {Promise<Object>} Sólo catálogo de repositorios, correlacionado con su suscripción. */
    async repositoriesRead(message) {
      if (message.originService !== 'service-apigateway')
        throw new ApplicationError(403, 'FORBIDDEN');
      const context = messaging.getContext();
      if (!context.delegation) throw new ApplicationError(403, 'FORBIDDEN');
      return {
        statusCode      : 200,
        descriptionCode : 'OK',
        data            : await windows.read(
          {
            ...message.data,
            topic   : 'documents.repository',
            section : 'collection',
            query   : {},
          },
          {
            userCode  : context.user?.userCode,
            workspace : context.workspace,
            context,
          },
        ),
      };
    },
    /** @param {string} topic Dominio. @param {Object} change Hecho durable. @returns {Promise<void>} Actualiza las ventanas del proceso. */
    changed(topic, change) {
      return windows.changed(topic, change);
    },
    /** @param {Object} message Petición con contexto acreditado por Gateway. @returns {Promise<Object>} Base y cursor exclusivamente HTTP. */
    async read(message) {
      if (message.originService !== 'service-apigateway')
        throw new ApplicationError(403, 'FORBIDDEN');
      const context = messaging.getContext();
      return {
        statusCode      : 200,
        descriptionCode : 'OK',
        data            : await windows.read(message.data, {
          userCode    : context.user?.userCode,
          sessionCode : context.sessionCode,
          workspace   : context.workspace,
          context,
        }),
      };
    },
  };
}
/** @returns {Object} Ventanas inicializadas antes de admitir conexiones. */
function realtimeWindows() {
  if (!windows) throw new Error('REALTIME_NOT_READY');
  return windows;
}
/** @returns {Object} Correlación compartida entre HTTP y socket de repositorios. */
function repositoryReads() {
  if (!repositories) throw new Error('REALTIME_NOT_READY');
  return repositories;
}
module.exports = {
  createRealtimeWindows,
  realtimeWindows,
  repositoryReads,
};
