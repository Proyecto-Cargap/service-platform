/** @file Bandejas autorizadas y trabajos durables; los modelos se inyectan y cada efecto transaccional usa sesión. */
'use strict';
const { AsyncLocalStorage } = require('node:async_hooks');
const { randomUUID }        = require('node:crypto');
const { ApplicationError }  = require('../../../shared/application-error');
const policy                = require('../policies/notifications.policy');
const INDEXES               = Object.freeze({
  PLATFORM_NOTIFICATION_BROADCAST : [
    {
      key    : { broadcastCode: 1 },
      name   : 'notification_broadcast_code',
      unique : true,
    },
  ],
  PLATFORM_NOTIFICATION_RECIPIENT : [
    {
      key    : {
        broadcastCode : 1,
        userCode      : 1,
      },
      name   : 'notification_recipient_unique',
      unique : true,
    },
  ],
  CONNECT_NOTIFICATIONS           : [
    {
      key                     : {
        broadcastCode : 1,
        userAssigned  : 1,
      },
      name                    : 'notification_broadcast_recipient',
      unique                  : true,
      partialFilterExpression : { broadcastCode: { $type: 'string' } },
    },
  ],
});

/**
 * Construye persistencia con filtros de acceso ya resueltos por el caso de uso local.
 * @param {Object} dependencies Modelos de notificación, trabajo y ledger y mensajería.
 * @returns {Object} Operaciones persistentes de bandeja y difusión.
 */
function createNotificationsCenterRepository({ notifications, broadcasts, recipients, messaging }) {
  const local   = new AsyncLocalStorage();
  const changes = require('../../../adapters/mongodb/platform-changes').platformChanges({
    messaging,
    connection : notifications.db,
  });
  /** @param {Function} work Consulta canónica completa. @returns {Promise<*>} Página y total en el mismo snapshot. */
  function snapshot(work) {
    if (local.getStore()) return work();
    return notifications.db.transaction(
      /** @param {Object} session Transacción de lectura. @returns {Promise<*>} Repositorios coherentes por operación. */
      (session) => local.run(session, work),
      { readConcern: { level: 'snapshot' } },
    );
  }
  /** @param {Object} filter Filtro canónico. @param {Object} values Modificación o null para borrar. @param {boolean} many Operación masiva propia. @returns {Promise<Object>} Cambios y hechos indivisibles. */
  function mutate(filter, values, many) {
    const query = guarded(filter);
    return changes.commit(
      /** @param {Object} session Unidad atómica. @returns {Promise<Object>} Resultado público original. */
      async (session) => {
        if (!many) {
          const item = values
            ? await notifications
                .findOneAndUpdate(
                  query,
                  {
                    $set : {
                      ...values,
                      updateDate : Date.now(),
                    },
                  },
                  {
                    session,
                    returnDocument : 'after',
                    runValidators  : true,
                  },
                )
                .lean()
            : await notifications.findOneAndDelete(query, { session }).lean();
          if (item) await changes.emit('notification', item, session, { erased: !values });
          return item;
        }
        const scopes = await notifications
          .aggregate([
            { $match: query },
            {
              $group : {
                _id : {
                  workspace : '$workspace',
                  userCode  : '$userCode',
                },
              },
            },
          ])
          .session(session);
        const result = values
          ? await notifications.updateMany(
              query,
              {
                $set : {
                  ...values,
                  updateDate : Date.now(),
                },
              },
              { session },
            )
          : await notifications.deleteMany(query, { session });
        if (result.modifiedCount || result.deletedCount)
          for (const row of scopes)
            await changes.emit('notification', row._id, session, { erased: !values });
        return result;
      },
    );
  }

  /** Exige índices únicos reales antes de aceptar o ejecutar una difusión.
   * @returns {Promise<void>} Finaliza cuando se completan los efectos de la operación.
   */
  async function indexesReady() {
    const hello = await notifications.db.db.admin().command({ hello: 1 });
    if ((!hello.setName && hello.msg !== 'isdbgrid') || !hello.logicalSessionTimeoutMinutes)
      throw new ApplicationError(503, 'NOTIFICATION_TRANSACTIONS_REQUIRED');
    for (const [collection, expected] of Object.entries(INDEXES)) {
      const actual = await notifications.db.db
        .collection(collection)
        .indexes()
        .catch(
          /**
           * Gestiona el fallo de la operación asíncrona y conserva la política de error del módulo.
           * @param {Error} error Fallo recibido por la continuación.
           * @returns {Array<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
           */
          (error) => {
            if (error.code === 26) return [];
            throw error;
          },
        );
      for (const index of expected) {
        const found = actual.find(
          /**
           * Determina si el elemento corresponde al recurso buscado.
           * @param {*} item Valor de item utilizado por esta operación.
           * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
           */
          (item) => item.name === index.name,
        );
        if (
          !found?.unique ||
          JSON.stringify(found.key) !== JSON.stringify(index.key) ||
          JSON.stringify(found.partialFilterExpression) !==
            JSON.stringify(index.partialFilterExpression)
        )
          throw new ApplicationError(503, 'NOTIFICATION_INDEXES_REQUIRED');
      }
    }
  }
  /** Sólo el marcador privado de este módulo cruza la política canónica de persistencia.
   * @param {Object} filter Filtros de selección que se autorizan antes de consultar.
   * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  function guarded(filter) {
    return policy.authorize('view', filter, policy.trustedScopes.guest, messaging.getContext());
  }
  return {
    indexesReady,
    snapshot,
    /** Devuelve la página y el conteo usando exactamente el mismo filtro construido localmente.
     * @param {Object} filter Filtros de selección que se autorizan antes de consultar.
     * @param {*} sort Valor de sort utilizado por esta operación.
     * @param {number} page Índice de la página según el contrato de esta operación.
     * @param {number} perPage Cantidad máxima de elementos por página.
     * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async list(filter, sort, page, perPage) {
      const query = guarded(filter);
      const items = await notifications
        .find(query)
        .select(
          'notificationCode broadcastCode workspace userCode userAssigned userCreate title content type viewed creationDate updateDate from link',
        )
        .sort(sort)
        .skip(page * perPage)
        .limit(perPage)
        .session(local.getStore() || null)
        .lean();
      return {
        items,
        pagination : {
          total : await notifications.countDocuments(query).session(local.getStore() || null),
          page  : page + 1,
          perPage,
        },
      };
    },
    /** Devuelve una notificación autorizada o null.
     * @param {Object} filter Filtros de selección que se autorizan antes de consultar.
     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    one(filter) {
      return notifications
        .findOne(guarded(filter))
        .select(
          'notificationCode broadcastCode workspace userCode userAssigned userCreate title content type viewed creationDate updateDate from link',
        )
        .session(local.getStore() || null)
        .lean();
    },
    /** Cuenta todo el conjunto autorizado, independientemente de la página.
     * @param {Object} filter Filtros de selección que se autorizan antes de consultar.
     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    count(filter) {
      return notifications.countDocuments(guarded(filter)).session(local.getStore() || null);
    },
    /** Marca o edita únicamente el conjunto autorizado.
     * @param {Object} filter Filtros de selección que se autorizan antes de consultar.
     * @param {Object} changes Valor de changes utilizado por esta operación.
     * @param {*} [many] Valor de many utilizado por esta operación.
     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    update(filter, values, many = false) {
      return mutate(filter, values, many);
    },
    /** La eliminación personal no elimina el ledger ni permite una entrega repetida.
     * @param {Object} filter Filtros de selección que se autorizan antes de consultar.
     * @param {*} [many] Valor de many utilizado por esta operación.
     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    remove(filter, many = false) {
      return mutate(filter, null, many);
    },
    /** Persiste una preparación completa antes de solicitar la confirmación.
     * @param {Object} input Entrada que se procesa con las reglas del módulo.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async prepare(input) {
      await indexesReady();
      return changes.commit(
        /** @param {Object} session Unidad de preparación. @returns {Promise<Object>} Difusión y hecho juntos. */
        async (session) => {
          const [job] = await broadcasts.create([input], { session });
          await changes.emit('broadcast', job, session, { created: true });
          return job;
        },
      );
    },
    /** Busca trabajos con filtros locales de acceso administrativo.
     * @param {Object} filter Filtros de selección que se autorizan antes de consultar.
     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    job(filter) {
      return broadcasts
        .findOne(filter)
        .session(local.getStore() || null)
        .lean();
    },
    /** Devuelve trabajos paginados sin la audiencia ni los tokens internos.
     * @param {Object} filter Filtros de selección que se autorizan antes de consultar.
     * @param {number} page Índice de la página según el contrato de esta operación.
     * @param {number} perPage Cantidad máxima de elementos por página.
     * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async jobs(filter, page, perPage) {
      return {
        items      : await broadcasts
          .find(filter)
          .select(
            'broadcastCode workspace userCreate title content type link state recipientCount persistedCount skippedCount failedCount pendingCount preparedAt confirmedAt completedAt updateDate confirmationToken',
          )
          .sort({
            preparedAt    : -1,
            broadcastCode : 1,
          })
          .skip(page * perPage)
          .limit(perPage)
          .session(local.getStore() || null)
          .lean(),
        pagination : {
          total : await broadcasts.countDocuments(filter).session(local.getStore() || null),
          page  : page + 1,
          perPage,
        },
      };
    },
    /** Confirma la audiencia o actualiza la preparación cuando cambió; todo el ledger confirma junto.
     * @param {*} broadcastCode Valor de broadcastCode utilizado por esta operación.
     * @param {*} audience Valor de audience utilizado por esta operación.
     * @param {*} audienceHash Valor de audienceHash utilizado por esta operación.
     * @param {*} confirmationToken Valor de confirmationToken utilizado por esta operación.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async confirm(broadcastCode, audience, audienceHash, confirmationToken) {
      await indexesReady();
      return broadcasts.db.transaction(
        /**
         * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
         * @param {Object} session Sesión que agrupa las operaciones de persistencia.
         * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
         */
        async (session) => {
          const job = await broadcasts.findOne({ broadcastCode }).session(session);
          if (!job) throw new ApplicationError(404, 'NOTIFICATION_BROADCAST_NOT_FOUND');
          if (job.state !== 'prepared')
            return {
              item            : job.toObject(),
              audienceChanged : false,
            };
          if (job.confirmationToken !== confirmationToken)
            return {
              item            : job.toObject(),
              audienceChanged : true,
            };
          if (job.audienceHash !== audienceHash) {
            job.audience = audience;
            job.audienceHash = audienceHash;
            job.confirmationToken = randomUUID();
            job.recipientCount = audience.length;
            job.updateDate = Date.now();
            await job.save({ session });
            await changes.emit('broadcast', job, session);
            return {
              item            : job.toObject(),
              audienceChanged : true,
            };
          }
          if (audience.length)
            await recipients.insertMany(
              audience.map(
                /**
                 * Proyecta cada elemento en la estructura requerida por la operación.
                 * @param {string} userCode Código del usuario relacionado con la operación.
                 * @returns {Object} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
                 */
                (userCode) => ({
                  broadcastCode,
                  workspace        : job.workspace,
                  userCreate       : job.userCreate,
                  userAssigned     : userCode,
                  userCode,
                  notificationCode : 'NOT-' + randomUUID(),
                }),
              ),
              { session },
            );
          job.state = audience.length ? 'queued' : 'completed';
          job.pendingCount = audience.length;
          job.confirmedAt = Date.now();
          job.completedAt = audience.length ? 0 : Date.now();
          job.audience = [];
          await job.save({ session });
          await changes.emit('broadcast', job, session);
          return {
            item            : job.toObject(),
            audienceChanged : false,
          };
        },
      );
    },
    /** Adquiere un trabajo y cerca procesos antiguos mediante un token aleatorio.
     * @param {*} [leaseMs] Valor de leaseMs utilizado por esta operación.
     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    claim(leaseMs = 30000) {
      return changes.commit(
        /** @param {Object} session Unidad de reclamación. @returns {Promise<Object|null>} Estado público y lease cercado. */
        async (session) => {
          const job = await broadcasts
            .findOneAndUpdate(
              {
                state        : { $in: ['queued', 'processing'] },
                pendingCount : { $gt: 0 },
                leaseUntil   : { $lte: Date.now() },
                $or          : [{ availableAt: { $lte: Date.now() } }, { availableAt: { $exists: false } }],
              },
              {
                $set : {
                  state      : 'processing',
                  leaseToken : randomUUID(),
                  leaseUntil : Date.now() + leaseMs,
                },
              },
              {
                session,
                sort           : { confirmedAt: 1 },
                returnDocument : 'after',
              },
            )
            .lean();
          if (job) await changes.emit('broadcast', job, session);
          return job;
        },
      );
    },
    /** Obtiene hasta cien destinatarios listos; los restantes permanecen durables.
     * @param {*} job Valor de job utilizado por esta operación.
     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    batch(job) {
      return recipients
        .find({
          broadcastCode : job.broadcastCode,
          state         : 'pending',
          availableAt   : { $lte: Date.now() },
        })
        .sort({ userCode: 1 })
        .limit(100)
        .lean();
    },
    /** Renueva sólo un lease aún propiedad del mismo proceso.
     * @param {*} job Valor de job utilizado por esta operación.
     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    renew(job) {
      return broadcasts.updateOne(
        {
          broadcastCode : job.broadcastCode,
          leaseToken    : job.leaseToken,
          leaseUntil    : { $gt: Date.now() },
        },
        { $set: { leaseUntil: Date.now() + 30000 } },
      );
    },
    /** Suelta propiedad conservando resultados y trabajo pendiente.
     * @param {*} job Valor de job utilizado por esta operación.
     * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async release(job) {
      return broadcasts.db.transaction(
        /** @param {Object} session Unidad atómica de agenda y liberación. @returns {Promise<Object>} Conserva el próximo vencimiento sin barrer pendientes. */
        async (session) => {
          const recipient = await recipients
            .findOne({
              broadcastCode : job.broadcastCode,
              state         : 'pending',
            })
            .sort({ availableAt: 1 })
            .select('availableAt')
            .session(session)
            .lean();
          const result    = await broadcasts.updateOne(
            {
              broadcastCode : job.broadcastCode,
              leaseToken    : job.leaseToken,
              state         : 'processing',
              pendingCount  : { $gt: 0 },
            },
            {
              $set : {
                leaseToken  : '',
                leaseUntil  : 0,
                state       : 'queued',
                availableAt : recipient?.availableAt ?? 0,
              },
            },
            { session },
          );
          if (result.modifiedCount) await changes.emit('broadcast', job, session);
          return result;
        },
      );
    },
    /** @returns {Promise<number|undefined>} Vencimiento real de pendientes o de una reclamación interrumpida. */
    async nextAt() {
      const [next] = await broadcasts.aggregate([
        {
          $match : {
            state        : { $in: ['queued', 'processing'] },
            pendingCount : { $gt: 0 },
          },
        },
        {
          $project : {
            deadline : { $max: [{ $ifNull: ['$leaseUntil', 0] }, { $ifNull: ['$availableAt', 0] }] },
          },
        },
        { $sort: { deadline: 1 } },
        { $limit: 1 },
      ]);
      return next?.deadline;
    },
    /** Entrega u omite un destinatario con notificación, ledger y contadores en un único commit cercado.
     * @param {*} job Valor de job utilizado por esta operación.
     * @param {*} recipient Valor de recipient utilizado por esta operación.
     * @param {*} eligible Valor de eligible utilizado por esta operación.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async persistRecipient(job, recipient, eligible) {
      return broadcasts.db.transaction(
        /**
         * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
         * @param {Object} session Sesión que agrupa las operaciones de persistencia.
         * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
         */
        async (session) => {
          const fenced = await broadcasts.findOneAndUpdate(
            {
              broadcastCode : job.broadcastCode,
              state         : 'processing',
              leaseToken    : job.leaseToken,
              leaseUntil    : { $gt: Date.now() },
            },
            { $set: { updateDate: Date.now() } },
            {
              session,
              returnDocument : 'after',
            },
          );
          if (!fenced) throw new ApplicationError(409, 'NOTIFICATION_LEASE_LOST');
          const row = await recipients
            .findOne({
              broadcastCode : job.broadcastCode,
              userCode      : recipient.userCode,
            })
            .session(session);
          if (!row || row.state !== 'pending') return null;
          if (eligible)
            await notifications.create(
              [
                {
                  notificationCode : row.notificationCode,
                  broadcastCode    : job.broadcastCode,
                  workspace        : job.workspace,
                  userCreate       : job.userCreate,
                  userAssigned     : row.userCode,
                  userCode         : row.userCode,
                  from             : job.userCreate,
                  title            : job.title,
                  content          : job.content,
                  type             : job.type,
                  link             : job.link,
                },
              ],
              { session },
            );
          if (eligible)
            await changes.emit(
              'notification',
              {
                notificationCode : row.notificationCode,
                workspace        : job.workspace,
                userCode         : row.userCode,
              },
              session,
              { created: true },
            );
          row.state = eligible ? 'persisted' : 'skipped';
          row.updateDate = Date.now();
          await row.save({ session });
          fenced.pendingCount--;
          if (eligible) fenced.persistedCount++;
          else fenced.skippedCount++;
          if (!fenced.pendingCount) {
            fenced.state = fenced.failedCount ? 'completed-with-errors' : 'completed';
            fenced.completedAt = Date.now();
            fenced.leaseUntil = 0;
            fenced.leaseToken = '';
          }
          await fenced.save({ session });
          await changes.emit('broadcast', fenced, session);
          return eligible
            ? {
                notificationCode : row.notificationCode,
                workspace        : job.workspace,
              }
            : null;
        },
      );
    },
    /** Reprograma únicamente fallos ciertos y limita los automáticos a tres reintentos.
     * @param {*} job Valor de job utilizado por esta operación.
     * @param {*} recipient Valor de recipient utilizado por esta operación.
     * @param {*} isTransient Valor de isTransient utilizado por esta operación.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async failRecipient(job, recipient, isTransient) {
      return broadcasts.db.transaction(
        /**
         * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
         * @param {Object} session Sesión que agrupa las operaciones de persistencia.
         * @returns {Promise<undefined>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
         */
        async (session) => {
          const fenced = await broadcasts.findOneAndUpdate(
            {
              broadcastCode : job.broadcastCode,
              state         : 'processing',
              leaseToken    : job.leaseToken,
              leaseUntil    : { $gt: Date.now() },
            },
            { $set: { updateDate: Date.now() } },
            {
              session,
              returnDocument : 'after',
            },
          );
          if (!fenced) return;
          const row = await recipients
            .findOne({
              broadcastCode : job.broadcastCode,
              userCode      : recipient.userCode,
              state         : 'pending',
            })
            .session(session);
          if (!row) return;
          row.attempts++;
          row.errorCode = isTransient
            ? 'DEPENDENCY_UNAVAILABLE'
            : 'NOTIFICATION_PERSISTENCE_FAILED';
          if (isTransient && row.attempts <= 3)
            row.availableAt = Date.now() + [1000, 5000, 30000][row.attempts - 1];
          else {
            row.state = 'failed';
            fenced.pendingCount--;
            fenced.failedCount++;
          }
          if (!fenced.pendingCount) {
            fenced.state = 'completed-with-errors';
            fenced.completedAt = Date.now();
            fenced.leaseUntil = 0;
            fenced.leaseToken = '';
          }
          await row.save({ session });
          await fenced.save({ session });
          await changes.emit('broadcast', fenced, session);
        },
      );
    },
    /** Reintenta sólo resultados fallidos; los persistidos nunca vuelven a crearse.
     * @param {*} broadcastCode Valor de broadcastCode utilizado por esta operación.
     * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
     */
    async retry(broadcastCode) {
      return broadcasts.db.transaction(
        /**
         * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
         * @param {Object} session Sesión que agrupa las operaciones de persistencia.
         * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
         */
        async (session) => {
          const job = await broadcasts.findOne({ broadcastCode }).session(session);
          if (!job) throw new ApplicationError(404, 'NOTIFICATION_BROADCAST_NOT_FOUND');
          if (job.state !== 'completed-with-errors')
            throw new ApplicationError(409, 'NOTIFICATION_BROADCAST_NOT_RETRYABLE');
          await recipients.updateMany(
            {
              broadcastCode,
              state : 'failed',
            },
            {
              $set : {
                state       : 'pending',
                attempts    : 0,
                availableAt : 0,
                errorCode   : '',
              },
            },
            { session },
          );
          job.pendingCount = job.failedCount;
          job.failedCount = 0;
          job.state = 'queued';
          job.completedAt = 0;
          job.leaseToken = '';
          job.leaseUntil = 0;
          job.availableAt = 0;
          await job.save({ session });
          await changes.emit('broadcast', job, session);
          return job.toObject();
        },
      );
    },
  };
}
module.exports = createNotificationsCenterRepository;
module.exports.INDEXES = INDEXES;
