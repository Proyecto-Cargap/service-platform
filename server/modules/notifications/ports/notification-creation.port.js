/** @file Capacidades mínimas para crear notificaciones sin resolver destinatarios, leer bandejas ni construir infraestructura. */
'use strict';
/**
 * @typedef {Object} NotificationCreationDependencies
 * @property {Object} repository Persistencia autorizada con create(input, scope). Devuelve una promesa de {notificationCode, workspace} simple, sin documentos Mongoose. Un error anterior a confirmar rechaza y no acredita publicación.
 * @property {Object} identifiers Generador con getObjectCode('NOT'); devuelve un código nuevo o rechaza antes de escribir.
 * @property {Object} messaging Contexto acreditado mediante getContext(); el caso de uso no lee identidad del contenido recibido.
 * @property {Object} logger Observador con error(metadata), sin datos privados ni contenido del aviso.
 */

/** Valida las capacidades obligatorias al componer el caso de uso sin invocarlas.
 * @param {NotificationCreationDependencies} dependencies Colaboradores ya construidos.
 * @returns {NotificationCreationDependencies} Conserva la identidad de todas las capacidades sustituidas.
 * @throws {TypeError} INVALID_CAPABILITY antes de ejecutar contexto, persistencia o transporte.
 */
function validateNotificationCreation(dependencies) {
  for (const [name, methods] of Object.entries({
    repository  : ['create'],
    identifiers : ['getObjectCode'],
    messaging   : ['getContext'],
    logger      : ['error'],
  })) {
    for (const method of methods) {
      if (typeof dependencies?.[name]?.[method] !== 'function')
        throw new TypeError(`INVALID_CAPABILITY:${name}.${method}`);
    }
  }
  return dependencies;
}
module.exports = { validateNotificationCreation };
