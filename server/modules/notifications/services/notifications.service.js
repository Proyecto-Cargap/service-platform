/** @file Creación reutilizable de notificaciones y aviso canónico después de persistir. */
'use strict';
const { ApplicationError }             = require('../../../shared/application-error');
const policy                           = require('../policies/notifications.policy');
const { validateNotificationCreation } = require('../ports/notification-creation.port');
const { id }                           = require('../../../../package.json');
const controller                       = 'C001';
/** Compone la creación con persistencia, identificadores y distribución ya construidos.
 * @param {Object} dependencies Capacidades mínimas de la creación de notificaciones.
 * @returns {Object} Operación create sin estado compartido, conexiones ni SDK.
 */
module.exports = function notificationsService(dependencies) {
  const { messaging, repository, identifiers, logger } = validateNotificationCreation(dependencies);
  return {
    /** Persiste una notificación autorizada y publica una invalidación con su identidad estable.
     * @param {Object} data Destinatario, espacio de trabajo y contenido público.
     * @returns {Promise<{notificationCode:string,workspace:string}>} Referencia canónica creada.
     * @throws {ApplicationError} Conserva permisos y marca la entrega incierta posterior a persistir.
     */
    async create(data) {
      let document;

      try {
        if (
          !data ||
          typeof data !== 'object' ||
          Array.isArray(data) ||
          !data.alertObject ||
          typeof data.alertObject !== 'object' ||
          Array.isArray(data.alertObject)
        )
          throw new ApplicationError(400, 'INVALID_NOTIFICATION');
        if (
          !policy.authorize(
            'create',
            {
              workspace    : data.workspace,
              userAssigned : data.userCode || '',
            },
            data.scope === undefined ? '' : data.scope,
            messaging.getContext(),
          )
        )
          throw new ApplicationError(403, 'FORBIDDEN');
        if (data.userCode == undefined || data.userCode == '') {
          throw new ApplicationError(400, `${id}-${controller}-E0001:userCode is required`);
        }

        {
          const userCode = data.userCode;
          const title    = data.alertObject.title;
          const type     = data.alertObject.type;
          const content  = data.alertObject.content ? data.alertObject.content : null;
          const action   = data.alertObject.action ? data.alertObject.action : null;
          const link     = data.alertObject.link ? data.alertObject.link : null;
          const idObject = data.alertObject.idObject ? data.alertObject.idObject : null;
          const from     = data.alertObject.from ? data.alertObject.from : '';

          document = await repository.create(
            {
              notificationCode : await identifiers.getObjectCode('NOT'),
              userAssigned     : data.userCode,
              workspace        : data.workspace,
              userCode         : userCode,
              title            : title,
              type             : type,
              content          : content,
              link             : link,
              action           : action,
              idObject         : idObject,
              from             : from,
            },
            data.scope === undefined ? '' : data.scope,
          );
        }

        return document;
      } catch (err) {
        if (err instanceof ApplicationError) throw err;

        logger.error({
          component : 'application',
          event     : 'operation_failed',
        });
        throw new ApplicationError(500, 'INTERNAL_ERROR', { cause: err });
      }
    },
  };
};
