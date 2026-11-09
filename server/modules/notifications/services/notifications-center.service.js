/** @file Bandeja propia autenticada y administración de notificaciones y difusiones con permisos exactos. */
'use strict';
const { createHash, randomUUID } = require('node:crypto');
const { ApplicationError }       = require('../../../shared/application-error');
const functions                  = require('../../../shared/general');
const policy                     = require('../policies/notifications.policy');

/**
 * Construye los casos de uso de notificaciones sin estado de solicitud compartido.
 * @param {Object} dependencies Mensajería y repositorio.
 * @returns {Object} Operaciones personales, administrativas y de difusión.
 */
function notificationsCenterService({
  messaging,
  repository,

  logger = {
    /**
     * Ofrece el punto de extensión error sin efectos por defecto.
     * @returns {void} Finaliza después de aplicar los efectos de la operación.
     */
    error() {},
  },
}) {
  /** Obtiene identidad vigente directamente de Security, sin datos de identidad del navegador.
   * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  async function actor() {
    const result = await messaging.request('service-security', 'identity-get-current', {});
    if (result.statusCode !== 200)
      throw new ApplicationError(result.statusCode || 503, 'NOTIFICATION_ACCESS_UNAVAILABLE');
    return {
      ...result.data,
      user : { userCode: result.data.userCode },
    };
  }
  /** Valida cadenas utilizadas en filtros, contenido y códigos.
   * @param {*} value Valor de value utilizado por esta operación.
   * @param {number} maximum Valor de maximum utilizado por esta operación.
   * @param {*} [required] Valor de required utilizado por esta operación.
   * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  function text(value, maximum, required = false) {
    if (typeof value !== 'string' || value.length > maximum || (required && !value.trim()))
      throw new ApplicationError(400, 'INVALID_INPUT');
    return value.trim();
  }
  /** Valida sólo las propiedades públicas editables y una ruta de aplicación sin traversal.
   * @param {Object} data Datos de entrada de la operación.
   * @param {*} [partial] Valor de partial utilizado por esta operación.
   * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  function content(data, partial = false) {
    const result = {};
    for (const [field, maximum] of [
      ['title', 160],
      ['content', 10000],
      ['type', 40],
    ]) {
      if (partial && data[field] === undefined) continue;
      result[field] =
        field === 'type' && data[field] === undefined ? 'info' : text(data[field], maximum, true);
    }
    if (!partial || data.link !== undefined) {
      result.link = null;
      if (data.link !== undefined && data.link !== null) {
        const link = data.link;
        if (
          !link ||
          typeof link !== 'object' ||
          Array.isArray(link) ||
          typeof link.route !== 'string' ||
          link.route.length > 300 ||
          !/^\/app\/[A-Za-z0-9][A-Za-z0-9/_-]*$/.test(link.route) ||
          link.route.includes('//') ||
          (link.parameters !== undefined &&
            (!Array.isArray(link.parameters) ||
              link.parameters.length > 10 ||
              link.parameters.some(
                /**
                 * Comprueba si el elemento satisface la condición.
                 * @param {*} item Valor de item utilizado por esta operación.
                 * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
                 */
                (item) => typeof item !== 'string' || item.length > 200,
              )))
        )
          throw new ApplicationError(400, 'INVALID_NOTIFICATION_LINK');
        result.link = {
          route      : link.route,
          ...(link.idObject !== undefined ? { idObject: text(link.idObject, 200) } : {}),
          parameters : link.parameters || [],
        };
      }
    }
    return result;
  }
  /** Agrega únicamente filtros documentados sin aceptar consultas MongoDB del cliente.
   * @param {Object} data Datos de entrada de la operación.
   * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  function filters(data) {
    const condition = {};
    if (data.search !== undefined && text(data.search, 300)) {
      const search = new RegExp(functions.escapeRegex(data.search.trim()), 'i');
      condition.$or = [{ title: search }, { content: search }];
    }
    if (data.viewed !== undefined && data.viewed !== '') {
      if (![true, false, 'true', 'false'].includes(data.viewed))
        throw new ApplicationError(400, 'INVALID_INPUT');
      condition.viewed = data.viewed === true || data.viewed === 'true';
    }
    if (data.type !== undefined && data.type !== '') condition.type = text(data.type, 40, true);
    if (data.userCode !== undefined && data.userCode !== '')
      condition.userCode = text(data.userCode, 100, true);
    if (data.code !== undefined) condition.notificationCode = text(data.code, 100, true);
    return condition;
  }
  /** Limita las páginas y los campos de ordenación a los visibles de esta bandeja.
   * @param {Object} data Datos de entrada de la operación.
   * @returns {Object} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  function pagination(data) {
    const { page, perPage } = functions.getPublicPagination(data);
    if (!perPage || perPage > 100) throw new ApplicationError(400, 'INVALID_PAGINATION');
    const field = data.sort || 'creationDate';
    const order = data.order || 'desc';
    if (
      !['creationDate', 'updateDate', 'title', 'type'].includes(field) ||
      !['asc', 'desc'].includes(order)
    )
      throw new ApplicationError(400, 'INVALID_SORT');
    return {
      page,
      perPage,
      sort : {
        [field]          : order === 'asc' ? 1 : -1,
        notificationCode : 1,
      },
    };
  }
  /** Conserva toda notificación propia autorizada sin usar permisos assigned ni alcance global.
   * @param {Object} data Datos de entrada de la operación.
   * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  async function personal(data) {
    const identity = await actor();
    const selected = data.workspace || identity.workspace;
    if (
      selected !== undefined &&
      selected !== '' &&
      selected !== 'allWorkspaces' &&
      !identity.workspaces.includes(selected)
    )
      throw new ApplicationError(403, 'WORKSPACE_FORBIDDEN');
    return {
      $and : [
        filters(data),
        {
          userAssigned : identity.userCode,
          userCode     : identity.userCode,
          workspace    : {
            $in :
              selected && selected !== 'allWorkspaces'
                ? [selected, 'allWorkspaces']
                : [...identity.workspaces, 'allWorkspaces'],
          },
        },
      ],
    };
  }
  /** Resuelve por acción un permiso exacto global o workspace sobre metadatos canónicos.
   * @param {string} action Valor de action utilizado por esta operación.
   * @param {Object} data Datos de entrada de la operación.
   * @param {*} [extra] Valor de extra utilizado por esta operación.
   * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  async function administrative(action, data, extra = {}) {
    const identity = await actor();
    const scope    =
      data.scope ||
      (identity.policies.includes(`platform.notifications.${action}.global`)
        ? 'global'
        : 'workspace');
    if (!['global', 'workspace'].includes(scope)) throw new ApplicationError(403, 'FORBIDDEN');
    const condition = {
      ...filters(data),
      ...extra,
    };
    if (data.workspace && data.workspace !== 'allWorkspaces')
      condition.workspace = text(data.workspace, 100, true);
    const authorized = policy.authorize(action, condition, scope, identity);
    if (!authorized) throw new ApplicationError(403, 'FORBIDDEN');
    return {
      identity,
      scope,
      filter : authorized,
    };
  }
  /** Proyecta campos públicos positivos sin transportar documentos o metadatos arbitrarios.
   * @param {*} item Valor de item utilizado por esta operación.
   * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  function visible(item) {
    if (!item) throw new ApplicationError(404, 'NOTIFICATION_NOT_FOUND');
    const value  = item.toObject ? item.toObject() : item;
    const fields = value.notificationCode
      ? [
          'notificationCode',
          'broadcastCode',
          'workspace',
          'userCode',
          'userAssigned',
          'userCreate',
          'title',
          'content',
          'type',
          'viewed',
          'creationDate',
          'updateDate',
          'from',
          'link',
        ]
      : [
          'broadcastCode',
          'workspace',
          'userCreate',
          'title',
          'content',
          'type',
          'link',
          'state',
          'recipientCount',
          'persistedCount',
          'skippedCount',
          'failedCount',
          'pendingCount',
          'preparedAt',
          'confirmedAt',
          'completedAt',
          'updateDate',
        ];
    const result = Object.fromEntries(
      fields
        .filter(
          /**
           * Determina si el elemento cumple el criterio de selección.
           * @param {string} field Campo utilizado por la operación.
           * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
           */
          (field) => value[field] !== undefined,
        )
        .map(
          /**
           * Proyecta cada elemento en la estructura requerida por la operación.
           * @param {string} field Campo utilizado por la operación.
           * @returns {Array<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
           */
          (field) => [field, value[field]],
        ),
    );
    if (!value.notificationCode && value.state === 'prepared')
      result.confirmationToken = value.confirmationToken;
    if (value.notificationCode) result.sourceKind = value.broadcastCode ? 'broadcast' : 'event';
    if (result.link && typeof result.link === 'object')
      result.link = {
        route      : result.link.route,
        idObject   : result.link.idObject || '',
        parameters : Array.isArray(result.link.parameters) ? result.link.parameters : [],
      };
    return result;
  }
  /** Exige autorización administrativa por workspace antes de consultar o mutar un trabajo.
   * @param {string} action Valor de action utilizado por esta operación.
   * @param {Object} data Datos de entrada de la operación.
   * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  async function jobAccess(action, data) {
    const job = await repository.job({ broadcastCode: text(data.code, 100, true) });
    if (!job) throw new ApplicationError(404, 'NOTIFICATION_BROADCAST_NOT_FOUND');
    const { identity, scope } = await administrative(action, {
      ...data,
      workspace : job.workspace,
    });
    if (scope === 'workspace' && job.workspace !== identity.workspace)
      throw new ApplicationError(403, 'FORBIDDEN');
    return job;
  }
  /** Obtiene una audiencia actual y su huella estable, sin límite silencioso.
   * @param {string} workspace Espacio de trabajo que delimita la operación.
   * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  async function audience(workspace) {
    const current = await actor();
    if (!current.workspaces.includes(workspace))
      throw new ApplicationError(403, 'WORKSPACE_FORBIDDEN');
    const result    = await functions.collectPages(
      /**
       * Ejecuta la continuación local y propaga el resultado al flujo que la invoca.
       * @param {*} pagination Valor de pagination utilizado por esta operación.
       * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      async (pagination) => {
        const response = await messaging.request('service-security', 'workspace-users-get-list', {
          workspace,
          ...pagination,
        });
        if (response.statusCode !== 200 || !Array.isArray(response.data?.items))
          throw new ApplicationError(
            response.statusCode || 503,
            'NOTIFICATION_AUDIENCE_UNAVAILABLE',
          );
        return response.data;
      },
    );
    const userCodes = [
      ...new Set(
        result.items.map(
          /**
           * Proyecta cada elemento en la estructura requerida por la operación.
           * @param {*} item Valor de item utilizado por esta operación.
           * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
           */
          (item) => item.userCode,
        ),
      ),
    ].sort();
    return {
      workspace,
      userCodes,
      hash : createHash('sha256').update(JSON.stringify(userCodes)).digest('hex'),
    };
  }
  /** Resuelve referencias de una página ya autorizada; un fallo de presentación conserva sus filas.
   * @param {Array<*>} items Elementos que se recorren o transforman.
   * @param {string} field Campo utilizado por la operación.
   * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  async function profilesFor(items, field) {
    const codes = [
      ...new Set(
        items
          .map(
            /**
             * Proyecta cada elemento en la estructura requerida por la operación.
             * @param {*} item Valor de item utilizado por esta operación.
             * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
             */
            (item) => item[field],
          )
          .filter(
            /**
             * Determina si el elemento cumple el criterio de selección.
             * @param {string} code Identificador público del recurso.
             * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
             */
            (code) => typeof code === 'string' && code && code.length <= 100,
          ),
      ),
    ];
    if (!codes.length) return new Map();
    let response;
    try {
      response = await messaging.request('service-security', 'user-profile-get-list', {
        codes,
      });
    } catch (error) {
      if ([401, 403].includes(error?.statusCode)) throw error;
      return new Map();
    }
    if ([401, 403].includes(response?.statusCode))
      throw new ApplicationError(response.statusCode, 'NOTIFICATION_ACCESS_UNAVAILABLE');
    if (response?.statusCode !== 200 || !Array.isArray(response.data?.items)) return new Map();
    return new Map(
      response.data.items
        .filter(
          /**
           * Determina si el elemento cumple el criterio de selección.
           * @param {*} profile Valor de profile utilizado por esta operación.
           * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
           */
          (profile) => profile && codes.includes(profile.userCode),
        )
        .map(
          /**
           * Proyecta cada elemento en la estructura requerida por la operación.
           * @param {*} profile Valor de profile utilizado por esta operación.
           * @returns {Array<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
           */
          (profile) => [profile.userCode, profile],
        ),
    );
  }
  /** Enriquece autores personales sin mezclar su identidad con la del destinatario.
   * @param {Array<*>} items Elementos que se recorren o transforman.
   * @param {string} workspace Espacio de trabajo que delimita la operación.
   * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  async function withSenders(items, workspace) {
    const profiles = await profilesFor(items, 'userCreate');
    return items.map(
      /**
       * Proyecta cada elemento en la estructura requerida por la operación.
       * @param {*} item Valor de item utilizado por esta operación.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      (item) => {
        const profile = profiles.get(item.userCreate);
        if (typeof profile?.name !== 'string' || !profile.name.trim()) return item;
        return {
          ...item,
          sender : {
            name : profile.name,
            ...(profile.hasPicture === true
              ? {
                  pictureUrl : `/platform/notifications/me/${encodeURIComponent(item.notificationCode)}/sender/picture?workspace=${encodeURIComponent(workspace || item.workspace)}`,
                }
              : {}),
          },
        };
      },
    );
  }
  /** Expone sólo el nombre del contacto de cada destinatario de la página administrativa.
   * @param {Array<*>} items Elementos que se recorren o transforman.
   * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  async function withRecipients(items) {
    const profiles = await profilesFor(items, 'userCode');
    return items.map(
      /**
       * Proyecta cada elemento en la estructura requerida por la operación.
       * @param {*} item Valor de item utilizado por esta operación.
       * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
       */
      (item) => {
        const profile = profiles.get(item.userCode);
        return typeof profile?.name === 'string' && profile.name.trim()
          ? {
              ...item,
              recipient : { name: profile.name.trim() },
            }
          : item;
      },
    );
  }
  const operations = {};
  Object.assign(
    operations,
    require('./notifications-personal-workflow.service').notificationsPersonalService({
      ApplicationError,
      messaging,
      pagination,
      personal,
      repository,
      text,
      visible,
      withSenders,
    }),
  );
  Object.assign(
    operations,
    require('./notifications-administration-workflow.service').notificationsAdministrationService({
      administrative,
      jobAccess,
      pagination,
      repository,
      visible,
      withRecipients,
    }),
  );
  Object.assign(
    operations,
    require('./notifications-broadcast-workflow.service').notificationsBroadcastService({
      ApplicationError,
      actor,
      administrative,
      audience,
      content,
      jobAccess,
      pagination,
      randomUUID,
      repository,
      text,
      visible,
    }),
  );
  return operations;
}
module.exports = notificationsCenterService;
