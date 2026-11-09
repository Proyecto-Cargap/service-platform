/** @file Comprueba permisos, catálogo publicado, DTO y aislamiento del almacenamiento de instaladores. */
'use strict';
const test                                = require('node:test');
const assert                              = require('node:assert/strict');
const { mkdtemp, writeFile, symlink, rm } = require('node:fs/promises');
const os                                  = require('node:os');
const path                                = require('node:path');
const {
  desktopReleaseService,
} = require('../server/modules/desktop-release/services/desktop-release.service');
const { desktopReleaseCatalog } = require('../server/adapters/files/desktop-release.catalog');
const dtos                      = require('../server/modules/desktop-release/dtos/desktop-release.actions.dto');
/** @returns {Object} Metadatos sintéticos de una publicación. */
function fixture() {
  return {
    version     : '0.1.1',
    publishedAt : '2026-11-06T12:00:00.000Z',
    notes       : 'Prueba',
    artifacts   : [
      {
        target    : 'mac-arm64',
        filename  : 'Xidden-Documentos-0.1.1-mac-arm64.dmg',
        bytes     : 10,
        sha256    : 'a'.repeat(64),
        minimumOs : 'macOS 14',
        publisher : 'Editor de prueba',
        signature : 'developer-id-notarized',
      },
    ],
  };
}
test('exige sesión antes de leer y nunca interpreta errores como catálogo vacío' /** @returns {Promise<void>} Verifica ausencia, fallos y autorización. */, async () => {
  let reads     = 0;
  let context   = {};
  let value     = fixture();
  const service = desktopReleaseService({
    /** @returns {Object} Contexto controlado. */
    getContext : () => context,
    /** @returns {Promise<Object|null>} Lectura instrumentada. */
    async readCurrent() {
      reads++;
      return value;
    },
  });
  await assert.rejects(service.current(), { statusCode: 401 });
  assert.equal(reads, 0);
  context = {
    user        : { userCode: 'USER' },
    sessionCode : 'SESSION',
  };
  assert.deepEqual(await service.current(), value);
  assert.equal(reads, 1);
  await assert.rejects(
    service.download({
      version : '0.1.0',
      target  : 'mac-arm64',
    }),
    { statusCode: 409 },
  );
  await assert.rejects(
    service.download({
      version : '0.1.1',
      target  : 'win-x64',
    }),
    { statusCode: 404 },
  );
  assert.equal(
    (
      await service.download({
        version : '0.1.1',
        target  : 'mac-arm64',
      })
    ).artifact.filename,
    value.artifacts[0].filename,
  );
  value = null;
  assert.equal(await service.current(), null);
  await assert.rejects(
    service.download({
      version : '0.1.1',
      target  : 'mac-arm64',
    }),
    { statusCode: 404 },
  );
});
test('rechaza paquetes sin firma, duplicados, nombres arbitrarios y contratos ajenos' /** @returns {Promise<void>} Valida la publicación antes de ofrecerla. */, async () => {
  for (const mutate of [
    /** @param {Object} value Manifiesto sintético. @returns {void} Simula paquete de piloto. */
    (value) => {
      value.artifacts[0].signature = 'ad-hoc';
    },
    /** @param {Object} value Manifiesto sintético. @returns {void} Simula duplicado. */
    (value) => {
      value.artifacts.push(value.artifacts[0]);
    },
    /** @param {Object} value Manifiesto sintético. @returns {void} Simula ruta no permitida. */
    (value) => {
      value.artifacts[0].filename = '../secret';
    },
    /** @param {Object} value Manifiesto sintético. @returns {void} Simula firma equivocada para el sistema. */
    (value) => {
      value.artifacts[0].signature = 'authenticode';
    },
  ]) {
    const value = fixture();
    mutate(value);
    const service = desktopReleaseService({
      /** @returns {Object} Sesión válida de prueba. */
      getContext  : () => ({
        user        : { userCode: 'USER' },
        sessionCode : 'SESSION',
      }),
      /** @returns {Promise<Object>} Manifiesto alterado. */
      readCurrent : async () => value,
    });
    await assert.rejects(service.current(), { statusCode: 503 });
  }
  assert.throws(
    /** @returns {Object} Intenta inyectar una proyección ajena. */
    () => dtos['desktop-release-current'].input({ consumer: 'web' }),
    { statusCode: 400 },
  );
  assert.throws(
    /** @returns {Object} Intenta escapar de la versión. */
    () =>
      dtos['desktop-release-download'].input({
        version : '../',
        target  : 'mac-arm64',
      }),
    { statusCode: 400 },
  );
  const output = dtos['desktop-release-current'].output({
    statusCode : 200,
    data       : { release: fixture() },
  });
  assert.equal(output.data.release.version, '0.1.1');
});
test('el lector real distingue ausencia, corrupción y enlaces simbólicos' /** @returns {Promise<void>} Utiliza sólo un directorio temporal propio. */, async () => {
  const root     = await mkdtemp(path.join(os.tmpdir(), 'desktop-catalog-'));
  const realRoot = await require('node:fs/promises').realpath(root);
  const read     = desktopReleaseCatalog(realRoot);
  try {
    assert.equal(await read(), null);
    await writeFile(path.join(root, 'current.json'), JSON.stringify(fixture()));
    assert.deepEqual(await read(), fixture());
    await writeFile(path.join(root, 'current.json'), '{');
    await assert.rejects(read(), { statusCode: 503 });
    await rm(path.join(root, 'current.json'));
    await writeFile(path.join(root, 'outside.json'), JSON.stringify(fixture()));
    await symlink(path.join(root, 'outside.json'), path.join(root, 'current.json'));
    await assert.rejects(read(), { statusCode: 503 });
  } finally {
    await rm(root, {
      recursive : true,
      force     : true,
    });
  }
});
