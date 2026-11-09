/** @file Hechos durables de Billing: entrega diferencias autorizadas por el único canal realtime. */
'use strict';
const { ApplicationError } = require('../../../shared/application-error');

/** @param {Object} dependencies Ventanas registradas del proceso. @returns {Object} Consumidor sin conexiones ni datos fiscales. */
function billingEvents({ windows }) {
  return {
    /** @param {Object} message Evento validado por DTO. @param {string} resource Recurso canónico. @returns {Promise<void>} Cambios de las ventanas afectadas. */
    changed(message, resource = 'document') {
      if (message.originService !== 'service-billing') throw new ApplicationError(403, 'FORBIDDEN');
      return windows.changed('billing.' + resource, message.data);
    },
  };
}
module.exports = { billingEvents };
