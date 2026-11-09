/** @file Comprueba que la preparación declarada de MongoDB permite registrar versiones. */
'use strict';
const test                    = require('node:test');
const assert                  = require('node:assert/strict');
const createReleaseRepository = require('../server/modules/release/repositories/release.repository');
const { collections }         = require('../server/mongodb-indexes.json');
const { INDEXES }             = require('../server/modules/release/contracts/release.contract');
const { matchesIndex }        = require('../server/library/messaging/indexes');
const releaseModel            = require('../server/modules/release/models/release.model');

/**
 * Reproduce la creación del repositorio con los índices suministrados, sin abrir una conexión.
 * @param {Object[]} indexes Índices que declara la preparación de la colección.
 * @returns {{ repository: Object, writes: Object[] }} Repositorio y documentos enviados a persistencia.
 */
function fixture(indexes) {
  const writes = [];
  /** Modelo aislado que registra únicamente las escrituras que superan las precondiciones. */
  class DraftModel {
    /** @param {Object} input Documento autorizado por la política del repositorio. */
    constructor(input) {
      this.input = input;
    }
    /** @returns {Promise<Object>} Documento persistido por el doble, sin efectos externos. */
    async save() {
      writes.push(this.input);
      return this.input;
    }
    static db = {
      /** @param {Function} work Unidad. @returns {Promise<*>} Sesión sintética de la prueba de índices. */ async transaction(
        work,
      ) {
        return work({ id: 'release-write' });
      },
    };
    /** @param {Array} inputs Documentos. @param {Object} options Sesión. @returns {Promise<Array>} Escritura posterior a la comprobación de índices. */
    static async create(inputs, options) {
      assert.equal(options.session.id, 'release-write');
      writes.push(...inputs);
      return inputs;
    }
    static collection = {
      /** @returns {Promise<Object[]>} Índices disponibles para la comprobación real del repositorio. */
      async indexes() {
        return indexes;
      },
    };
  }
  return {
    repository : createReleaseRepository({
      model     : DraftModel,
      messaging : {
        /** @param {string} name Hecho. @param {Object} data Referencia. @param {Object} options Unidad. @returns {Promise<void>} Cambio y outbox comparten sesión. */ async publishEvent(
          name,
          data,
          options,
        ) {
          assert.equal(name, 'platform.resource.changed');
          assert.equal(data.resource, 'release');
          assert.equal(options.session.id, 'release-write');
        },
        /** @returns {Object} Contexto autenticado con el permiso global de creación. */
        getContext() {
          return {
            user     : { userCode: 'test-user' },
            policies : ['platform.release.create.global'],
          };
        },
      },
    }),
    writes,
  };
}

test('el manifiesto de MongoDB permite registrar un borrador autorizado' /** @returns {Promise<void>} Comprueba que la preparación declarada supera el bloqueo de índices. */, async () => {
  const { repository, writes } = fixture(collections.PLATFORM_RELEASE);
  const draft                  = await repository.createDraft({
    workspace   : 'allWorkspaces',
    version     : '1.0.0',
    description : 'Novedades de prueba',
    state       : '0-development',
    releaseDate : 0,
  });
  assert.equal(writes.length, 1);
  assert.equal(draft.version, '1.0.0');
  assert.equal(draft.userCreate, 'test-user');
});

test('sin los índices de integridad se rechaza la creación antes de escribir' /** @returns {Promise<void>} Conserva la protección ante una base sin preparar. */, async () => {
  const { repository, writes } = fixture([
    {
      key  : { _id: 1 },
      name : '_id_',
    },
  ]);
  await assert.rejects(repository.createDraft({ workspace: 'allWorkspaces' }), {
    statusCode      : 503,
    descriptionCode : 'RELEASE_INDEXES_REQUIRED',
  });
  assert.equal(writes.length, 0);
});

test('el esquema y el manifiesto conservan los índices parciales exigidos al escribir' /** @returns {void} Verifica nombres, claves, unicidad y filtros en las tres fuentes. */, () => {
  for (const expected of INDEXES) {
    assert(
      collections.PLATFORM_RELEASE.some(
        /** @param {Object} index Índice preparado administrativamente. @returns {boolean} Indica si coincide con la precondición de escritura. */
        (index) => matchesIndex(index, expected),
      ),
    );
    assert(
      releaseModel.schema.indexes().some(
        /** @param {Array} entry Claves y opciones declaradas en Mongoose. @returns {boolean} Indica si coincide con el contrato de integridad. */
        ([key, options]) =>
          matchesIndex(
            {
              key,
              ...options,
            },
            expected,
          ),
      ),
    );
  }
});
