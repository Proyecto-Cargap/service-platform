/** @file Persistencia local de release; el catálogo proporciona el modelo. */
'use strict';
const { ApplicationError }  = require('../../../shared/application-error');
const policy                = require('../policies/release.policy');
const { AsyncLocalStorage } = require('node:async_hooks');
const {
  FIELDS,
  INDEXES,
  globalFilter,
  publishedRelease,
  releaseText,
} = require('../contracts/release.contract');

/**
 * Ejecuta releaseError dentro del flujo de release.repository.
 * @param {Error} error Fallo recibido por la continuación.
 * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
function releaseError(error) {
  if (error instanceof ApplicationError) return error;
  if (error.code === 11000) return new ApplicationError(409, 'RELEASE_CONFLICT');
  if (error.code === 20) return new ApplicationError(503, 'RELEASE_TRANSACTIONS_REQUIRED');
  return new ApplicationError(500, 'INTERNAL_ERROR', { cause: error });
}

/**
 * Compone createReleaseRepository con las dependencias recibidas y devuelve su interfaz.
 * @param {Object} options Colaboradores u opciones: model, messaging.
 * @returns {Object} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
 */
module.exports = function createReleaseRepository({ model, messaging }) {
  const local  = new AsyncLocalStorage();
  const events = require('../../../adapters/mongodb/platform-changes').platformChanges({
    messaging,
    connection : model.db,
  });
  /** @param {Object} input Documento autorizado. @returns {Promise<Object>} Alta y hecho en la misma transacción. */
  function save(input) {
    return events.commit(
      /** @param {Object} session Transacción. @returns {Promise<Object>} Documento confirmado. */ async (
        session,
      ) => {
        const [item] = await model.create([input], { session });
        await events.emit('release', item, session, { created: true });
        return item;
      },
    );
  }
  /** @param {Object} filter Selector autorizado. @param {Object|null} values Cambio o baja física. @param {boolean} many Operación masiva. @returns {Promise<Object>} Cambio indivisible con su hecho. */
  function change(filter, values, many = false) {
    return events.commit(
      /** @param {Object} session Unidad de escritura. @returns {Promise<Object>} Resultado canónico. */ async (
        session,
      ) => {
        const item = many
          ? await (values
              ? model.updateMany(filter, values, {
                  session,
                  runValidators : true,
                })
              : model.deleteMany(filter, { session }))
          : await (values
              ? model.findOneAndUpdate(filter, values, {
                  session,
                  returnDocument : 'after',
                  runValidators  : true,
                })
              : model.findOneAndDelete(filter, { session }));
        if (many ? item.modifiedCount || item.deletedCount : item)
          await events.emit(
            'release',
            many
              ? {
                  code      : '',
                  workspace : 'allWorkspaces',
                }
              : item,
            session,
            { erased: !values || values.erased === true || values.$set?.erased === true },
          );
        return item;
      },
    );
  }

  /**
   * Ejecuta indexesReady dentro del flujo de release.repository.
   * @returns {Promise<void>} Finaliza cuando se completan los efectos de la operación.
   */
  async function indexesReady() {
    const indexes = await model.collection.indexes().catch(
      /**
       * Gestiona el fallo de la operación asíncrona y conserva la política de error del módulo.
       * @returns {Array<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      () => [],
    );
    for (const expected of INDEXES) {
      const actual = indexes.find(
        /**
         * Determina si el elemento corresponde al recurso buscado.
         * @param {number} index Valor de index utilizado por esta operación.
         * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
         */
        (index) => index.name === expected.name,
      );
      if (
        !actual ||
        !actual.unique ||
        JSON.stringify(actual.key) !== JSON.stringify(expected.key) ||
        JSON.stringify(actual.partialFilterExpression) !==
          JSON.stringify(expected.partialFilterExpression)
      )
        throw new ApplicationError(503, 'RELEASE_INDEXES_REQUIRED');
    }
  }
  /**
   * Comprueba permisos y delimita el alcance de la operación.
   * @param {string} action Valor de action utilizado por esta operación.
   * @param {Object} filter Filtros de selección que se autorizan antes de consultar.
   * @param {Object} changes Valor de changes utilizado por esta operación.
   * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  function authorize(action, filter, changes) {
    const authorized = policy.authorize(action, filter, 'global', {
      ...messaging.getContext(),
      changes,
    });
    if (!authorized) throw new ApplicationError(403, 'FORBIDDEN');
    return authorized;
  }
  return {
    /** @param {Function} work Lectura canónica completa. @returns {Promise<*>} Página y total del mismo snapshot. */
    snapshot(work) {
      if (local.getStore()) return work();
      return model.db.transaction(
        /** @param {Object} session Transacción de lectura. @returns {Promise<*>} Consulta sobre la misma instantánea. */ (
          session,
        ) => local.run(session, work),
        { readConcern: { level: 'snapshot' } },
      );
    },
    /** Lee exclusivamente la versión global vigente; el consumidor autoriza la sesión.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async currentPublished() {
      const value = await model
        .findOne(globalFilter({ state: '1-current' }), FIELDS)
        .session(local.getStore() || null)
        .lean();
      return value ? publishedRelease(value) : null;
    },
    /**
     * Ejecuta publishedHistory dentro del flujo de release.repository.
     * @param {number} page Índice de la página según el contrato de esta operación.
     * @param {number} perPage Cantidad máxima de elementos por página.
     * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async publishedHistory(page, perPage) {
      const filter = globalFilter({ state: '2-released' });
      const items  = await model
        .find(filter, FIELDS)
        .sort({
          releaseDate : -1,
          releaseCode : -1,
        })
        .skip(page * perPage)
        .limit(perPage)
        .session(local.getStore() || null)
        .lean();
      return {
        items      : items.map(publishedRelease),
        pagination : {
          total : await model.countDocuments(filter).session(local.getStore() || null),
          page  : page + 1,
          perPage,
        },
      };
    },
    /**
     * Compone createDraft con las dependencias recibidas y devuelve su interfaz.
     * @param {Object} input Entrada que se procesa con las reglas del módulo.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async createDraft(input) {
      const authorized = authorize('create', input);
      await indexesReady();
      try {
        return await save(authorized);
      } catch (error) {
        throw releaseError(error);
      }
    },
    /**
     * Ejecuta updateRelease dentro del flujo de release.repository.
     * @param {Object} filter Filtros de selección que se autorizan antes de consultar.
     * @param {Object} changes Valor de changes utilizado por esta operación.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async updateRelease(filter, changes) {
      const authorized = authorize('update', filter, changes);
      await indexesReady();
      try {
        return await change(authorized, { $set: changes });
      } catch (error) {
        throw releaseError(error);
      }
    },
    /** Serializa publicaciones en una transacción y comprueba soporte e índices antes de escribir.
     * @param {Object} filter Filtros de selección que se autorizan antes de consultar.
     * @param {*} webBuild Valor de webBuild utilizado por esta operación.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async publishRelease(filter, webBuild) {
      const authorized = authorize('update', filter);
      await indexesReady();
      const hello = await model.db.db.admin().command({ hello: 1 });
      if ((!hello.setName && hello.msg !== 'isdbgrid') || !hello.logicalSessionTimeoutMinutes)
        throw new ApplicationError(503, 'RELEASE_TRANSACTIONS_REQUIRED');
      try {
        return await events.commit(
          /**
           * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
           * @param {Object} session Sesión que agrupa las operaciones de persistencia.
           * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
           */
          async (session) => {
            const lock = await model.db
              .collection('PLATFORM_RELEASE_CONTROL')
              .updateOne({ _id: 'global' }, { $inc: { revision: 1 } }, { session });
            if (!lock.matchedCount) throw new ApplicationError(503, 'RELEASE_INDEXES_REQUIRED');
            const item = await model.findOne(authorized).session(session);
            if (!item) throw new ApplicationError(404, 'RELEASE_NOT_FOUND');
            if (item.state === '1-current')
              return {
                item,
                changed : false,
              };
            if (item.state !== '0-development')
              throw new ApplicationError(409, 'RELEASE_PUBLISHED');
            if (!webBuild) throw new ApplicationError(400, 'RELEASE_CLIENT_UPDATE_REQUIRED');
            releaseText(item.version, 'version', 120);
            releaseText(item.description, 'description', 20000);
            const now = Date.now();
            await model.updateMany(
              globalFilter({ state: '1-current' }),
              {
                $set : {
                  state      : '2-released',
                  updateDate : now,
                },
              },
              { session },
            );
            const saved = await model.findOneAndUpdate(
              authorized,
              {
                $set : {
                  state       : '1-current',
                  releaseDate : now,
                  updateDate  : now,
                  webBuild,
                },
              },
              {
                session,
                returnDocument     : 'after',
                runValidators      : true,
                overwriteImmutable : true,
              },
            );
            await events.emit(
              'release',
              {
                code      : '',
                workspace : 'allWorkspaces',
              },
              session,
            );
            return {
              item    : saved,
              changed : true,
            };
          },
        );
      } catch (error) {
        throw releaseError(error);
      }
    },
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
      return save(authorizedInput);
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
      return model.findOne(authorizedFilter).session(local.getStore() || null);
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
      const pageQuery  = model
        .find(authorizedFilter, fields)
        .skip(perPage * page)
        .limit(perPage)
        .sort(sort)
        .session(local.getStore() || null);
      const listItem   = await pageQuery;
      const totalCount = await model
        .countDocuments(authorizedFilter)
        .session(local.getStore() || null);
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
      return authorizedFilter
        ? model.countDocuments(authorizedFilter).session(local.getStore() || null)
        : 0;
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
      return change(authorizedFilter, changes);
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
      const saved = await change(filter, changes);
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
      if (!isLogical) return change(authorizedFilter, null);
      return change(
        { $and: [authorizedFilter, { erased: { $ne: true } }] },
        {
          $set : {
            erased     : true,
            updateDate : Date.now(),
          },
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
      if (!isLogical) return change(authorizedFilter, null, true);
      const result = await change(
        { $and: [authorizedFilter, { erased: { $ne: true } }] },
        {
          $set : {
            erased     : true,
            updateDate : Date.now(),
          },
        },
        true,
      );
      return {
        acknowledged : result.acknowledged,
        deletedCount : result.modifiedCount,
      };
    },
  };
};
