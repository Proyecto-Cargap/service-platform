/** @file Vincula una instalación móvil a la identidad autenticada sin aceptar identidades del cuerpo. */
'use strict';
const { ApplicationError } = require('../../../shared/application-error');

/**
 * Compone el registro con persistencia y validación de sesión sustituibles.
 * @param {Object} dependencies Repositorio, lectura autenticada, reloj y disponibilidad.
 * @returns {Object} Operación register; nunca devuelve la dirección de Firebase.
 */
function createPushRegistration({ repository, currentIdentity, now, available }) {
  return {
    /**
     * Registra una revisión monotónica de la instalación; una respuesta perdida admite repetirla.
     * @param {Object} data Instalación, vinculación, generación, plataforma y FID.
     * @param {Object} context Contexto del transporte, nunca del cuerpo HTTP.
     * @returns {Promise<Object>} Vinculación confirmada para la sesión y espacio vigentes.
     */
    async register(data, context) {
      if (!available) throw new ApplicationError(503, 'PUSH_UNAVAILABLE');
      const identity = await currentIdentity(context);
      if (!identity || identity.userCode !== context.user?.userCode)
        throw new ApplicationError(401, 'UNAUTHORIZED');
      if (
        !identity.policies.some(
          /** Comprueba una capacidad que admite avisos del dispositivo.
           * @param {string} value Política confirmada por Security.
           * @returns {boolean} Existe autorización de lectura.
           */
          (value) =>
            /^(communication\.chat|customer\.activity)\.view\.(global|workspace|owner|assigned)$/.test(
              value,
            ),
        )
      )
        throw new ApplicationError(403, 'FORBIDDEN');
      await repository.register({
        installationId : data.installationId,
        bindingId      : data.bindingId,
        generation     : data.generation,
        platform       : data.platform,
        fid            : data.fid,
        userCode       : identity.userCode,
        sessionCode    : context.sessionCode,
        workspace      : identity.workspace,
        updatedAt      : now(),
      });
      return { bindingId: data.bindingId };
    },
  };
}
module.exports = { createPushRegistration };
