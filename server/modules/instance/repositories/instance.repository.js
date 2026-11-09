/** @file Persistencia local de instance; el catálogo proporciona el modelo. */
'use strict';
const { ApplicationError } = require('../../../shared/application-error');
const policy               = require('../policies/instance.policy');

/**
 * Compone createInstanceRepository con las dependencias recibidas y devuelve su interfaz.
 * @param {Object} options Colaboradores u opciones: model, messaging.
 * @returns {Object} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
module.exports = function createInstanceRepository({ model, messaging }) {
  return {
    /** Guarda una entidad nueva y devuelve el documento hidratado.
     * @param {Object} input Entrada que se procesa con las reglas del módulo.
     * @param {string|symbol} [scope] Alcance solicitado o marcador interno de confianza.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async create(input, scope = '') {
      if (!input || typeof input !== 'object' || Array.isArray(input))
        throw new ApplicationError(400, 'INVALID_INPUT');
      const authorizedInput = policy.authorize('create', input, scope, messaging.getContext());
      if (!authorizedInput) throw new ApplicationError(403, 'FORBIDDEN');
      const document = new model(authorizedInput);
      return document.save();
    },
    /** Busca con el filtro completo; la ausencia se representa con null.
     * @param {Object} filter Filtros de selección que se autorizan antes de consultar.
     * @param {string|symbol} [scope] Alcance solicitado o marcador interno de confianza.
     * @param {*} [operation] Valor de operation utilizado por esta operación.
     * @param {Object} changes Valor de changes utilizado por esta operación.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async getOne(filter, scope = '', operation = 'view', changes) {
      if (!filter || typeof filter !== 'object' || Array.isArray(filter))
        throw new ApplicationError(400, 'INVALID_FILTER');
      const authorizedFilter = policy.authorize(operation, filter, scope, {
        ...messaging.getContext(),
        changes,
      });
      if (!authorizedFilter) {
        if (operation !== 'view') throw new ApplicationError(403, 'FORBIDDEN');
        return null;
      }
      return model.findOne(authorizedFilter);
    },
    /** Página desde cero; perPage=0 mantiene la consulta sin límite.
     * @param {Object} filter Filtros de selección que se autorizan antes de consultar.
     * @param {*} [sort] Valor de sort utilizado por esta operación.
     * @param {number} [page] Índice de la página según el contrato de esta operación.
     * @param {number} [perPage] Cantidad máxima de elementos por página.
     * @param {*} [fields] Valor de fields utilizado por esta operación.
     * @param {string|symbol} [scope] Alcance solicitado o marcador interno de confianza.
     * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async getList(filter, sort = {}, page = 0, perPage = 20, fields = '', scope = '') {
      if (!filter || typeof filter !== 'object' || Array.isArray(filter))
        throw new ApplicationError(400, 'INVALID_FILTER');
      if (!sort || typeof sort !== 'object' || Array.isArray(sort))
        throw new ApplicationError(400, 'INVALID_SORT');
      if (
        !Number.isInteger(page) ||
        page < 0 ||
        !Number.isInteger(perPage) ||
        perPage < 0 ||
        perPage > 10000
      )
        throw new ApplicationError(400, 'INVALID_PAGINATION');
      if (
        typeof fields !== 'string' &&
        (!fields || typeof fields !== 'object' || Array.isArray(fields))
      )
        throw new ApplicationError(400, 'INVALID_FIELDS');
      const authorizedFilter = policy.authorize('view', filter, scope, messaging.getContext());
      if (!authorizedFilter)
        return {
          listItem   : [],
          totalCount : 0,
        };
      const pageQuery              = model
        .find(authorizedFilter, fields)
        .skip(perPage * page)
        .limit(perPage)
        .sort({
          ...sort,
          _id : 1,
        });
      const countQuery             = model.countDocuments(authorizedFilter);
      const [listItem, totalCount] = await Promise.all([pageQuery, countQuery]);
      return {
        listItem,
        totalCount,
      };
    },
    /** Cuenta todas las coincidencias del filtro, sin paginación.
     * @param {Object} filter Filtros de selección que se autorizan antes de consultar.
     * @param {string|symbol} [scope] Alcance solicitado o marcador interno de confianza.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async count(filter, scope = '') {
      if (!filter || typeof filter !== 'object' || Array.isArray(filter))
        throw new ApplicationError(400, 'INVALID_FILTER');
      const authorizedFilter = policy.authorize('view', filter, scope, messaging.getContext());
      return authorizedFilter ? model.countDocuments(authorizedFilter) : 0;
    },
    /** Devuelve la versión nueva del documento y aplica las reglas del esquema.
     * @param {Object} filter Filtros de selección que se autorizan antes de consultar.
     * @param {Object} changes Valor de changes utilizado por esta operación.
     * @param {string|symbol} [scope] Alcance solicitado o marcador interno de confianza.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async update(filter, changes, scope = '') {
      if (!filter || typeof filter !== 'object' || Array.isArray(filter))
        throw new ApplicationError(400, 'INVALID_FILTER');
      if (!changes || typeof changes !== 'object' || Array.isArray(changes))
        throw new ApplicationError(400, 'INVALID_CHANGES');
      const authorizedFilter = policy.authorize('update', filter, scope, {
        ...messaging.getContext(),
        changes,
      });
      if (!authorizedFilter) throw new ApplicationError(403, 'FORBIDDEN');
      return model.findOneAndUpdate(authorizedFilter, changes, {
        returnDocument : 'after',
        runValidators  : true,
      });
    },
    /** Revalida la autorización atómicamente y registra la fecha de modificación.
     * @param {*} document Valor de document utilizado por esta operación.
     * @param {string|symbol} [scope] Alcance solicitado o marcador interno de confianza.
     * @param {*} [operation] Valor de operation utilizado por esta operación.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async updateItem(document, scope = '', operation = 'update') {
      if (!document || typeof document !== 'object' || Array.isArray(document) || !document._id)
        throw new ApplicationError(400, 'INVALID_DOCUMENT');
      const changes =
        typeof document.toObject === 'function' ? document.toObject() : { ...document };
      delete changes._id;
      delete changes.__v;
      delete changes.save;
      changes.updateDate = Date.now();
      const filter = policy.authorize(operation, { _id: document._id }, scope, {
        ...messaging.getContext(),
        changes,
      });
      if (!filter) throw new ApplicationError(403, 'FORBIDDEN');
      const saved = await model.findOneAndUpdate(filter, changes, {
        returnDocument : 'after',
        runValidators  : true,
      });
      if (!saved) throw new ApplicationError(403, 'FORBIDDEN');
      return saved;
    },
    /** La baja lógica es el valor por defecto; false selecciona eliminación física.
     * @param {Object} filter Filtros de selección que se autorizan antes de consultar.
     * @param {*} [isLogical] Valor de isLogical utilizado por esta operación.
     * @param {string|symbol} [scope] Alcance solicitado o marcador interno de confianza.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async deleteOne(filter, isLogical = true, scope = '') {
      if (typeof isLogical !== 'boolean') throw new ApplicationError(400, 'INVALID_DELETE_MODE');
      if (!filter || typeof filter !== 'object' || Array.isArray(filter))
        throw new ApplicationError(400, 'INVALID_FILTER');
      const authorizedFilter = policy.authorize('delete', filter, scope, messaging.getContext());
      if (!authorizedFilter) throw new ApplicationError(403, 'FORBIDDEN');
      if (!isLogical) return model.findOneAndDelete(authorizedFilter);
      return model.findOneAndUpdate(
        { $and: [authorizedFilter, { erased: { $ne: true } }] },
        {
          $set : {
            erased     : true,
            updateDate : Date.now(),
          },
        },
        {
          returnDocument : 'after',
          runValidators  : true,
        },
      );
    },
    /** Devuelve el número de registros eliminados físicamente o dados de baja por primera vez.
     * @param {Object} filter Filtros de selección que se autorizan antes de consultar.
     * @param {*} [isLogical] Valor de isLogical utilizado por esta operación.
     * @param {string|symbol} [scope] Alcance solicitado o marcador interno de confianza.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async deleteMany(filter, isLogical = true, scope = '') {
      if (typeof isLogical !== 'boolean') throw new ApplicationError(400, 'INVALID_DELETE_MODE');
      if (!filter || typeof filter !== 'object' || Array.isArray(filter))
        throw new ApplicationError(400, 'INVALID_FILTER');
      const authorizedFilter = policy.authorize('delete', filter, scope, messaging.getContext());
      if (!authorizedFilter) throw new ApplicationError(403, 'FORBIDDEN');
      if (!isLogical) return model.deleteMany(authorizedFilter);
      const result = await model.updateMany(
        { $and: [authorizedFilter, { erased: { $ne: true } }] },
        {
          $set : {
            erased     : true,
            updateDate : Date.now(),
          },
        },
        { runValidators: true },
      );
      return {
        acknowledged : result.acknowledged,
        deletedCount : result.modifiedCount,
      };
    },
    /** Sólo comprueba colisiones de código; nunca concede acceso al registro.
     * @param {*} instanceCode Valor de instanceCode utilizado por esta operación.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async hasCode(instanceCode) {
      if (typeof instanceCode !== 'string' || !instanceCode)
        throw new ApplicationError(400, 'INVALID_INPUT');
      return Boolean(await model.exists({ instanceCode }));
    },
  };
};
