/** @file Catálogo global de versiones: lectura autenticada y administración con permisos exactos. */
'use strict';
const { ApplicationError }                            = require('../../../shared/application-error');
const functions                                       = require('../../../shared/general');
const { publishedRelease, globalFilter, releaseText } = require('../contracts/release.contract');

module.exports = class ReleaseService {
  #document = null;

  /** @param {Object} dependencies Contexto verificado, repositorio, identificadores y emisor local. */
  constructor({
    messaging,
    repository,
    identifiers,
    logger = {
      /**
       * Ofrece el punto de extensión error sin efectos por defecto.
       * @returns {void} Finaliza después de aplicar los efectos de la operación.
       */
      error() {},
    },
    verifyWebBuild,
  } = {}) {
    Object.assign(this, {
      messaging,
      repository,
      identifiers,
      logger,
      verifyWebBuild,
    });
  }

  /**
   * Ejecuta authenticated dentro del flujo de release.service.
   * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  authenticated() {
    const context = this.messaging.getContext();
    if (!context?.user?.userCode || !context.sessionCode || context.sessionCode === 'no session')
      throw new ApplicationError(401, 'UNAUTHORIZED');
    return context;
  }

  /**
   * Ejecuta administrator dentro del flujo de release.service.
   * @param {string} action Valor de action utilizado por esta operación.
   * @param {string|symbol} [scope] Alcance solicitado o marcador interno de confianza.
   * @returns {*} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  administrator(action, scope = '') {
    const context = this.authenticated();
    if (
      !['', 'global'].includes(scope) ||
      !context.policies?.includes(`platform.release.${action}.global`)
    )
      throw new ApplicationError(403, 'FORBIDDEN');
    return context;
  }

  /**
   * Crea el registro y devuelve el resultado de la operación.
   * @param {Object} data Datos de entrada de la operación.
   * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  async create(data) {
    const context     = this.administrator('create', data.scope);
    const version     = releaseText(data.version, 'version', 120);
    const description = releaseText(data.description, 'description', 20000);
    this.#document = await this.repository.createDraft({
      workspace     : 'allWorkspaces',
      userAssigned  : '',
      releaseCode   : await this.identifiers.getObjectCode('REL'),
      version,
      description,
      state         : '0-development',
      releaseDate   : 0,
      idTransaction : context.idTransaction,
    });
    return publishedRelease(this.#document);
  }

  /**
   * Obtiene el registro que cumple los filtros autorizados.
   * @param {Object} condition Valor de condition utilizado por esta operación.
   * @param {string|symbol} [scope] Alcance solicitado o marcador interno de confianza.
   * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  async getOne(condition, scope = '') {
    this.administrator('view', scope);
    this.#document = await this.repository.getOne(globalFilter(condition), 'global');
    return this.#document ? publishedRelease(this.#document) : null;
  }

  /**
   * Consulta una página de registros autorizados.
   * @param {Object} data Datos de entrada de la operación.
   * @returns {Promise<Object>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  async getList(data) {
    this.administrator('view', data.scope);
    if (data.search !== undefined && (typeof data.search !== 'string' || data.search.length > 200))
      throw new ApplicationError(400, 'INVALID_SEARCH');
    if (
      data.state !== undefined &&
      !['', '0-development', '1-current', '2-released'].includes(data.state)
    )
      throw new ApplicationError(400, 'INVALID_STATE');
    const sort  = data.sort === undefined ? 'creationDate' : data.sort;
    const order = data.order === undefined ? 'desc' : data.order;
    if (
      !['version', 'state', 'releaseDate', 'creationDate'].includes(sort) ||
      !['asc', 'desc'].includes(order)
    )
      throw new ApplicationError(400, 'INVALID_SORT');
    const { page, perPage } = functions.getPublicPagination(data);
    if (!perPage || perPage > 100) throw new ApplicationError(400, 'INVALID_PAGINATION');
    const condition = {};
    if (data.state) condition.state = data.state;
    if (data.search?.trim()) {
      const literal = new RegExp(functions.escapeRegex(data.search.trim()), 'i');
      condition.$or = [{ version: literal }, { description: literal }];
    }
    const result = await this.repository.getList(
      globalFilter(condition),
      {
        [sort]      : order === 'asc' ? 1 : -1,
        releaseCode : 1,
      },
      page,
      perPage,
      '',
      'global',
    );
    return {
      items      : result.listItem.map(publishedRelease),
      pagination : {
        total : result.totalCount,
        page  : page + 1,
        perPage,
      },
    };
  }

  /**
   * Ejecuta current dentro del flujo de release.service.
   * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  async current() {
    this.authenticated();
    return this.repository.currentPublished();
  }

  /**
   * Ejecuta history dentro del flujo de release.service.
   * @param {Object} data Datos de entrada de la operación.
   * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  async history(data) {
    this.authenticated();
    const { page, perPage } = functions.getPublicPagination(data);
    if (!perPage || perPage > 100) throw new ApplicationError(400, 'INVALID_PAGINATION');
    return this.repository.publishedHistory(page, perPage);
  }

  /**
   * Actualiza los campos autorizados del registro.
   * @param {Object} condition Valor de condition utilizado por esta operación.
   * @param {Object} data Datos de entrada de la operación.
   * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  async update(condition, data) {
    this.administrator('update', data.scope);
    const existing = await this.repository.getOne(globalFilter(condition), 'global', 'update');
    if (!existing) throw new ApplicationError(404, 'RELEASE_NOT_FOUND');
    const draft   = existing.state === '0-development';
    const allowed = draft
      ? ['code', 'scope', 'version', 'description']
      : ['code', 'scope', 'description'];
    if (
      Object.keys(data).some(
        /**
         * Comprueba si el elemento satisface la condición.
         * @param {string} key Clave del campo o recurso que se procesa.
         * @returns {boolean} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
         */
        (key) => !allowed.includes(key),
      )
    )
      throw new ApplicationError(400, 'RELEASE_FIELDS_LOCKED');
    const changes = { updateDate: Date.now() };
    if (data.version !== undefined) changes.version = releaseText(data.version, 'version', 120);
    if (data.description !== undefined)
      changes.description = releaseText(data.description, 'description', 20000);
    this.#document = await this.repository.updateRelease(
      globalFilter({
        ...condition,
        state : existing.state,
      }),
      changes,
    );
    if (!this.#document) throw new ApplicationError(409, 'RELEASE_CHANGED');
    return publishedRelease(this.#document);
  }

  /**
   * Elimina el recurso indicado según las reglas del módulo.
   * @param {Object} condition Valor de condition utilizado por esta operación.
   * @param {string|symbol} [scope] Alcance solicitado o marcador interno de confianza.
   * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  async delete(condition, scope = '') {
    this.administrator('delete', scope);
    const existing = await this.repository.getOne(globalFilter(condition), 'global', 'delete');
    if (!existing) throw new ApplicationError(404, 'RELEASE_NOT_FOUND');
    if (existing.state !== '0-development') throw new ApplicationError(409, 'RELEASE_PUBLISHED');
    this.#document = await this.repository.deleteOne(
      globalFilter({
        ...condition,
        state : '0-development',
      }),
      false,
      'global',
    );
    if (!this.#document) throw new ApplicationError(409, 'RELEASE_CHANGED');
    return publishedRelease(this.#document);
  }

  /**
   * Ejecuta webBuild dentro del flujo de release.service.
   * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  async webBuild() {
    this.administrator('update');
    try {
      return await this.verifyWebBuild();
    } catch (error) {
      this.logger.error({
        component : 'release',
        event     : 'web_build_verification_failed',
      });
      throw error;
    }
  }

  /**
   * Ejecuta published dentro del flujo de release.service.
   * @param {Object} condition Valor de condition utilizado por esta operación.
   * @param {string|symbol} [scope] Alcance solicitado o marcador interno de confianza.
   * @param {*} expectedBuildId Valor de expectedBuildId utilizado por esta operación.
   * @returns {Promise<*>} Resultado que utiliza el flujo que invoca esta función; los fallos se propagan según el contrato del módulo.
   */
  async published(condition, scope = '', expectedBuildId) {
    this.administrator('update', scope);
    const existing = await this.repository.getOne(globalFilter(condition), 'global', 'update');
    if (!existing) throw new ApplicationError(404, 'RELEASE_NOT_FOUND');
    let webBuild;
    if (existing.state !== '1-current') {
      if (existing.state !== '0-development') throw new ApplicationError(409, 'RELEASE_PUBLISHED');
      if (expectedBuildId === undefined)
        throw new ApplicationError(400, 'RELEASE_CLIENT_UPDATE_REQUIRED');
      if (
        typeof expectedBuildId !== 'string' ||
        !/^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$/.test(expectedBuildId)
      )
        throw new ApplicationError(400, 'INVALID_RELEASE_BUILD_ID');
      webBuild = await this.webBuild();
      if (webBuild.id !== expectedBuildId)
        throw new ApplicationError(409, 'RELEASE_WEB_BUILD_CHANGED');
    }
    const result = await this.repository.publishRelease(globalFilter(condition), webBuild);
    this.#document = result.item;
    return publishedRelease(result.item);
  }
};
