/**
 * @file Operaciones de aplicación de instancias. Coordina las validaciones y las colaboraciones
 * necesarias para cada solicitud.
 * Cada instancia conserva sus dependencias y devuelve el valor de la operación.
 */
'use strict';
const { ApplicationError } = require('../../../shared/application-error');
const policy               = require('../policies/instance.policy');
const functions            = require('../../../shared/general');
const { id }               = require('../../../../package.json');

const controller = 'C005';

/**
 * Implementa los casos de uso de instancias con dependencias por instancia.
 * Cada instancia conserva un documento de trabajo privado independiente.
 */
module.exports = class InstanceService {
  #document;
  /**
   * Conserva los colaboradores indicados e inicializa el documento de trabajo privado a null.
   *
   * @param {Object} [dependencies] Colaboradores que se conservan en esta instancia.
   * @param {Object} dependencies.messaging Contexto verificado y operaciones de mensajería.
   * @param {Object} [dependencies.logger] Registro técnico; por defecto omite la salida de errores.
   * @param {Object} dependencies.identifiers Generador de códigos de instancia.
   * @param {Object} dependencies.repository Repositorio de instancias.
   * @param {Object} dependencies.releases Repositorio usado para consultar la versión vigente.
   */
  constructor({
    messaging,

    logger = {
      /**
       * Ofrece el punto de extensión error sin efectos por defecto.
       * @returns {void} Finaliza después de aplicar los efectos de la operación.
       */
      error() {},
    },
    identifiers,
    repository,
    releases,
  } = {}) {
    this.messaging = messaging;
    this.logger = logger;

    this.identifiers = identifiers;
    this.releases = releases;
    this.repository = repository;
    this.#document = null;
  }

  /**
   * Prepara los datos de instancias y coordina la creación del registro y sus operaciones asociadas.
   *
   * @param {Object} data Datos de instancias que utiliza instanceCode, version, build, code.
   * @returns {Promise<Object|null>} Valor de la operación; devuelve null cuando no existe el recurso.
   */
  async create(data) {
    try {
      const system = this.messaging.getContext();
      const scope  = data.scope === undefined ? '' : data.scope;
      if (!policy.authorize('create', data, scope, system))
        throw new ApplicationError(403, 'FORBIDDEN');

      let currentVersion     = '0.0.0';
      let currentBuild       = '0';
      const currentVersionDB = await this.releases.getOne(
        {
          state     : '1-current',
          workspace : 'allWorkspaces',
          erased    : false,
        },
        require('../../release/policies/release.policy').trustedScopes.guest,
      );
      if (currentVersionDB) {
        currentVersion = currentVersionDB.version;
        currentBuild = currentVersionDB.build;
      }

      const instance =
        typeof data.instanceCode === 'string' &&
        data.instanceCode !== '' &&
        policy.authorize('update', {}, scope, system)
          ? await this.repository.getOne({ instanceCode: data.instanceCode }, scope, 'update')
          : null;

      if (instance) {
        instance.sessionCode = system.idSession;
        instance.instanceVersion.version = data.version != undefined ? data.version : '';
        instance.instanceVersion.build = data.build != undefined ? data.build : '';
        instance.currentVersion.version = currentVersion;
        instance.currentVersion.build = currentBuild;
        instance.latestVersion = data.version == currentVersion && data.build == currentBuild;
        instance.instanceVersion.idTransaction = system.idTransaction;

        await this.repository.updateItem(instance, scope);
        this.#document = instance;

        return this.#document;
      } else {
        if (data.instanceCode && (await this.repository.hasCode(data.instanceCode)))
          throw new ApplicationError(403, 'FORBIDDEN');
        const body = {
          workspace       : data.workspace,
          userAssigned    : data.userAssigned,
          sessionCode     : system.idSession,
          instanceCode    :
            data.instanceCode != undefined
              ? data.instanceCode
              : await this.identifiers.getObjectCode('INS'),
          domain          : data.code != undefined ? data.code : '',
          instanceVersion : {
            version : data.version != undefined ? data.version : '',
            build   : data.build != undefined ? data.build : '',
          },
          currentVersion  : {
            version : currentVersion,
            build   : currentBuild,
          },
          latestVersion   : data.version == currentVersion && data.build == currentBuild,
          idTransaction   : system.idTransaction,
        };

        this.#document = await this.repository.create(body, scope);

        return this.#document;
      }
    } catch (err) {
      if (err instanceof ApplicationError) throw err;

      this.logger.error({
        component : 'application',
        event     : 'operation_failed',
      });
      throw new ApplicationError(500, 'INTERNAL_ERROR', { cause: err });
    }
  }

  /**
   * Obtiene un registro de instancias y prepara los datos utilizados por la respuesta.
   *
   * @param {Object} condition Filtro de selección; puede modificarse al aplicar las restricciones de
   * consulta.
   * @returns {Promise<null|Object>} Valor de la operación; devuelve null cuando no existe el recurso.
   */
  async getOne(condition, scope = '') {
    try {
      this.#document = null;
      this.#document = await this.repository.getOne(condition, scope);
      if (!this.#document) {
        return null;
      }

      return this.#document;
    } catch (err) {
      if (err instanceof ApplicationError) throw err;

      this.logger.error({
        component : 'application',
        event     : 'operation_failed',
      });
      throw new ApplicationError(500, 'INTERNAL_ERROR', { cause: err });
    }
  }

  /**
   * Construye los filtros, orden y paginación para consultar instancias.
   *
   * @param {Object} data Datos de instancias que utiliza fields, sort, order, perPage, page, search,
   * dateRange, condition, version, build.
   * @returns {Promise<Object|null>} Valor de la operación; devuelve null cuando no existe el recurso.
   */
  async getList(data) {
    try {
      const paramFields = data.fields || '';
      const paramSort   = data.sort || null;
      const paramOrder  = data.order || null;

      const paramDateRange    = data.dateRange || null;
      const paramCondition    = data.condition || null;
      /**
       * Determina los campos y la dirección de ordenación de la consulta.
       */
      const sort              = await functions.getSortQuery('instanceCode', paramSort, paramOrder);
      /**
       * Convierte la página pública a base cero y valida el tamaño solicitado.
       */
      const { page, perPage } = functions.getPublicPagination(data);
      /**
       * Construye los filtros de consulta a partir de los parámetros de búsqueda recibidos.
       */
      const condition         = paramCondition ? paramCondition : {};
      if (!paramCondition) {
        condition.$and = [];
        condition.$and = [{ _id: { $ne: null } }];

        if (data.version != undefined) {
          condition.$and.push({ idObject: data.version });
        }

        if (data.build != undefined) {
          condition.$and.push({ build: data.build });
        }

        if (data.sessionCode != undefined) {
          condition.$and.push({ sessionCode: data.sessionCode });
        }

        if (data.instanceCode != undefined) {
          condition.$and.push({ instanceCode: data.instanceCode });
        }

        if (paramDateRange) {
          const date = paramDateRange.split('|');
          if (date[0] != '') {
            condition.$and.push({ creationDate: { $gte: date[0] } });
          }
          if (date[1] != '') {
            condition.$and.push({ creationDate: { $lte: date[1] } });
          }
        }
      }

      const result = await this.repository.getList(
        condition,
        sort,
        page,
        perPage,
        paramFields,
        data.scope === undefined ? '' : data.scope,
      );
      this.#document = {
        pagination : {
          total   : result.totalCount,
          page    : page + 1,
          perPage : perPage,
        },
        items      : result.listItem,
      };

      return this.#document;
    } catch (err) {
      if (err instanceof ApplicationError) throw err;

      this.logger.error({
        component : 'application',
        event     : 'operation_failed',
      });
      throw new ApplicationError(500, 'INTERNAL_ERROR', { cause: err });
    }
  }

  /**
   * Localiza el registro de instancias, aplica los cambios admitidos por esta operación y los persiste.
   *
   * @param {Object} condition Filtro de selección; puede modificarse al aplicar las restricciones de
   * consulta.
   * @param {Object} data Datos de instancias que utiliza sessionCode.
   * @returns {Promise<null|Object>} Valor de la operación; devuelve null cuando no existe el recurso.
   */
  async update(condition, data) {
    const scope = data.scope === undefined ? '' : data.scope;
    try {
      this.#document = null;
      this.#document = await this.repository.getOne(condition, scope, 'update');
      if (!this.#document) {
        throw new ApplicationError(404, `${id}-${controller}-E0003:Instance code is required`);
      }
      /**
       * Copia los campos utilizados por esta operación al documento que se va a guardar.
       */
      if (data.sessionCode != undefined) {
        this.#document.sessionCode = data.sessionCode;
      }

      await this.repository.updateItem(this.#document, scope);

      return this.#document;
    } catch (err) {
      if (err instanceof ApplicationError) throw err;

      this.logger.error({
        component : 'application',
        event     : 'operation_failed',
      });
      throw new ApplicationError(500, 'INTERNAL_ERROR', { cause: err });
    }
  }

  /**
   * Localiza y elimina el registro de instancias y coordina las operaciones asociadas presentes en este
   * flujo.
   *
   * @param {Object} condition Filtro de selección; puede modificarse al aplicar las restricciones de
   * consulta.
   * @returns {Promise<null|Object>} Valor de la operación; devuelve null cuando no existe el recurso.
   */
  async delete(condition, scope = '') {
    try {
      this.#document = null;
      this.#document = await this.repository.deleteOne(condition, false, scope);
      if (!this.#document) {
        throw new ApplicationError(404, `${id}-${controller}-E0003:Instance code is required`);
      }

      return this.#document;
    } catch (err) {
      if (err instanceof ApplicationError) throw err;

      this.logger.error({
        component : 'application',
        event     : 'operation_failed',
      });
      throw new ApplicationError(500, 'INTERNAL_ERROR', { cause: err });
    }
  }
};
