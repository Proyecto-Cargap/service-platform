/** @file Adaptación RPC de consultas autenticadas del catálogo de instaladores. */
'use strict';
const { desktopReleaseService } = require('../services/desktop-release.service');

/** @param {Object} dependencies Mensajería y lector de catálogo privado. @returns {Object} Handlers con contratos explícitos. */
function desktopReleaseController({ messaging, readCurrent }) {
  const service = desktopReleaseService({
    readCurrent,
    /** @returns {Object} Contexto de esta solicitud, nunca capturado entre usuarios. */
    getContext : () => messaging.getContext(),
  });
  return {
    /** @returns {Promise<Object>} Versión actual o ausencia explícita. */
    async current() {
      return messaging.successResponse(200, { release: await service.current() });
    },
    /** @param {Object} message Selección validada por el DTO. @returns {Promise<Object>} Metadatos autorizados para la transferencia. */
    async download(message) {
      return messaging.successResponse(200, await service.download(message.data));
    },
  };
}
module.exports = { desktopReleaseController };
