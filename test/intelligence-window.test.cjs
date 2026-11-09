/** @file Ventanas Intelligence: propiedad canónica, ámbito, representación cerrada y revocación. */
'use strict';
const { producerWindows } = require('./helpers/producer-window.cjs');
const test                = require('node:test');
const assert              = require('node:assert/strict');
const {
  intelligenceWindows,
} = require('../server/modules/realtime/services/intelligence-window.service');
const dtos = require('../server/contracts/registry');
test('una ventana sólo consulta el productor acreditado y rechaza datos privados o autoridad en la consulta', /** @returns {Promise<void>} Verifica el registro productivo con el contrato cerrado. */ async () => {
  const identity = {
    userCode  : 'u',
    workspace : 'w',
    context   : {
      user      : { userCode: 'u' },
      workspace : 'w',
      policies  : ['intelligence.run.view.workspace'],
    },
  };
  let response   = {
    statusCode : 200,
    data       : {
      runCode            : 'r',
      status             : 'queued',
      usage              : { status: 'unknown' },
      resultAvailability : 'pending',
    },
  };
  const windows  = producerWindows(intelligenceWindows, {
    messaging : {
      /** @param {string} service Productor. @param {string} action Consulta. @param {Object} query Selección. @param {Object} options Autoridad. @returns {Promise<Object>} Respuesta de contrato. */
      async request(service, action, query, options) {
        assert.equal(service, 'service-intelligence');
        assert.equal(action, 'run-get-one');
        assert.deepEqual(query, { runCode: 'r' });
        assert.equal(options.context, identity.context);
        return response;
      },
    },
  });
  const spec     = windows['intelligence.run'].detail;
  assert.deepEqual(spec.validate({ runCode: 'r' }), { runCode: 'r' });
  assert.throws(
    /** @returns {Object} Intenta introducir autoridad no confiable. */ () =>
      spec.validate({
        runCode  : 'r',
        userCode : 'foreign',
      }),
    { statusCode: 400 },
  );
  assert.equal((await spec.read({ runCode: 'r' }, identity)).data.runCode, 'r');
  response.data.configurationVersion = { secret: 'PRIVATE' };
  await assert.rejects(spec.read({ runCode: 'r' }, identity), { statusCode: 503 });
  response = {
    statusCode      : 403,
    descriptionCode : 'FORBIDDEN',
  };
  await assert.rejects(spec.read({ runCode: 'r' }, identity), { statusCode: 403 });
  assert.equal(
    spec.affects(
      { runCode: 'r' },
      {
        runCode   : 'other',
        workspace : 'w',
      },
      identity,
    ),
    false,
  );
  assert.equal(
    spec.affects(
      { runCode: 'r' },
      {
        runCode   : 'r',
        workspace : 'other',
      },
      identity,
    ),
    false,
  );
  assert.equal(
    spec.affects(
      {
        runCode : 'r',
        scope   : 'global',
      },
      {
        runCode   : 'r',
        workspace : 'other',
      },
      identity,
    ),
    true,
  );
  assert.equal(
    spec.affects(
      {},
      {
        control : {
          kind : 'user',
          code : 'u',
        },
      },
      identity,
    ),
    true,
  );
  assert.throws(
    /** @returns {Object} Rechaza contenido ajeno al hecho mínimo. */ () =>
      dtos['intelligence.run.changed'].input({
        runCode   : 'r',
        workspace : 'w',
        revision  : 1,
        result    : 'PRIVATE',
      }),
    { statusCode: 400 },
  );
});

test(
  'doce ventanas administrativas respetan identidad, consulta y contrato del mismo productor',
  {
    skip :
      !require('node:fs').existsSync(
        require('node:path').resolve(
          __dirname,
          '../../service-intelligence/server/contracts/service.json',
        ),
      ) && 'Requiere checkout conjunto de Intelligence',
  },
  /** @returns {Promise<void>} Página, ficha y configuración comparten el contrato público canónico. */ async () => {
    const producer = require('../../service-intelligence/server/contracts/service.json');
    const platform = require('../server/contracts/service.json').endpoints.find(
      /** @param {Object} endpoint Contrato. @returns {boolean} Lectura canónica. */ (endpoint) =>
        endpoint.name === 'realtime-read',
    );
    const identity = {
      userCode  : 'u',
      workspace : 'w',
      context   : {
        user      : { userCode: 'u' },
        workspace : 'w',
        policies  : ['intelligence.agent.view.workspace'],
      },
    };
    let expectedAction, expectedQuery, response;
    const windows = producerWindows(intelligenceWindows, {
      messaging : {
        /** @param {string} service Propietario. @param {string} action Operación. @param {Object} query Filtros. @param {Object} options Autoridad. @returns {Promise<Object>} Respuesta pública del ensayo. */
        async request(service, action, query, options) {
          assert.equal(service, 'service-intelligence');
          assert.equal(action, expectedAction);
          assert.deepEqual(query, expectedQuery);
          assert.equal(options.context, identity.context);
          return response;
        },
      },
    });
    for (const [resource, key] of [
      ['agent', 'agentCode'],
      ['team', 'teamCode'],
      ['tool', 'toolCode'],
      ['knowledge-base', 'knowledgeBaseCode'],
      ['trigger', 'triggerCode'],
      ['settings', 'parameter'],
    ]) {
      const topic = 'intelligence.' + resource;
      for (const section of [resource === 'settings' ? 'collection' : 'page', 'detail']) {
        expectedAction = resource + (section === 'detail' ? '-get-one' : '-get-list');
        expectedQuery =
          section === 'detail'
            ? { [key]: 'one' }
            : resource === 'settings'
              ? {}
              : {
                  page    : 1,
                  perPage : 20,
                  search  : 'test',
                };
        const source   = producer.endpoints.find(
          /** @param {Object} entry Contrato. @returns {boolean} Acción propietaria. */ (entry) =>
            entry.name === expectedAction,
        );
        const declared = platform.request.oneOf.find(
          /** @param {Object} entry Ventana. @returns {boolean} Misma sección. */ (entry) =>
            entry.properties.topic.enum[0] === topic &&
            entry.properties.section.enum[0] === section,
        );
        assert.deepEqual(declared.properties.query, source.request);
        const output = platform.successData.oneOf.find(
          /** @param {Object} entry Sobre. @returns {boolean} Misma sección. */ (entry) =>
            entry.properties.control.properties.topic.enum[0] === topic &&
            entry.properties.control.properties.section.enum[0] === section,
        ).properties.data;
        assert.deepEqual(output, source.successData);
        const row =
          resource === 'settings'
            ? {
                parameter : 'one',
                workspace : 'allWorkspaces',
                revision  : 1,
                data      : {
                  enabled               : false,
                  credentialsConfigured : {
                    apiKey : false,
                    token  : false,
                  },
                },
              }
            : {
                [key]        : 'one',
                workspace    : 'w',
                userCreate   : 'u',
                userAssigned : '',
                revision     : 1,
              };
        response = {
          statusCode : 200,
          data       :
            section === 'detail'
              ? row
              : resource === 'settings'
                ? [row]
                : {
                    items      : [row],
                    pagination : {
                      page       : 1,
                      perPage    : 20,
                      total      : 1,
                      totalPages : 1,
                    },
                  },
        };
        const spec = windows[topic][section];
        assert.deepEqual(spec.validate(expectedQuery), expectedQuery);
        assert.deepEqual((await spec.read(expectedQuery, identity)).data, response.data);
        const fact = {
          resource,
          code      : 'one',
          workspace : resource === 'settings' ? 'allWorkspaces' : 'w',
          revision  : 2,
          erased    : false,
        };
        assert.equal(spec.affects(expectedQuery, fact, identity), true);
        assert.equal(
          spec.affects(
            expectedQuery,
            {
              ...fact,
              resource : 'other',
            },
            identity,
          ),
          false,
        );
        if (section === 'detail')
          assert.equal(
            spec.affects(
              expectedQuery,
              {
                ...fact,
                code : 'other',
              },
              identity,
            ),
            false,
          );
        if (section === 'page')
          assert.equal(
            spec.affects(
              {
                ...expectedQuery,
                codes : ['selected'],
              },
              fact,
              identity,
            ),
            false,
          );
        response = {
          statusCode      : 503,
          descriptionCode : 'PRODUCER_UNAVAILABLE',
        };
        await assert.rejects(spec.read(expectedQuery, identity), { statusCode: 503 });
        response = { statusCode: 404 };
        await assert.rejects(spec.read(expectedQuery, identity), { statusCode: 403 });
      }
    }
    const dto = dtos['intelligence.resource.changed'];
    for (const field of ['data', 'token', 'apiKey', 'instructions'])
      assert.throws(
        /** @returns {Object} Los avisos mínimos no admiten contenido administrativo. */
        () =>
          dto.input({
            resource  : 'settings',
            code      : 'runtime',
            workspace : 'allWorkspaces',
            revision  : 1,
            erased    : false,
            [field]   : 'PRIVATE',
          }),
        { statusCode: 400 },
      );
  },
);
