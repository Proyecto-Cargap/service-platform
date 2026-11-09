/** @file Valida publicación atómica, recuperación idempotente, exclusión mutua e integridad sin tocar producción. */
'use strict';
const test               = require('node:test');
const assert             = require('node:assert/strict');
const fs                 = require('node:fs/promises');
const path               = require('node:path');
const os                 = require('node:os');
const { createHash }     = require('node:crypto');
const { installRelease } = require('../ops/install-desktop-release.cjs');

/** @param {Object} context Prueba propietaria del temporal. @returns {Promise<Object>} Directorios y catálogo sintético con bytes reales. */
async function fixture(context) {
  const base = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), 'desktop-publish-')));
  context.after(
    /** @returns {Promise<void>} Limpia únicamente los temporales de esta prueba. */ () =>
      fs.rm(base, {
        recursive : true,
        force     : true,
      }),
  );
  const input = path.join(base, 'input');
  const root  = path.join(base, 'private');
  await fs.mkdir(input);
  await fs.mkdir(root);
  const content  = Buffer.from('Instalador sintético para verificar la publicación, no ejecutable.');
  const artifact = {
    target    : 'mac-arm64',
    filename  : 'Xidden-Documentos-0.1.1-mac-arm64.dmg',
    bytes     : content.length,
    sha256    : createHash('sha256').update(content).digest('hex'),
    minimumOs : 'macOS 13',
    publisher : 'Prueba',
    signature : 'developer-id-notarized',
  };
  const release  = {
    version     : '0.1.1',
    publishedAt : '2026-11-06T12:00:00.000Z',
    notes       : '',
    artifacts   : [artifact],
  };
  await fs.writeFile(path.join(input, artifact.filename), content);
  await fs.writeFile(path.join(input, 'current.json'), JSON.stringify(release));
  return {
    input,
    root,
    release,
    artifact,
    content,
    expectedVersion : 'none',
  };
}
test('publica bytes íntegros y recupera un resultado incierto sin sustituir versiones', /** @param {Object} context Ciclo de vida. @returns {Promise<void>} Comprueba reintento y recuperación después de instalar bytes. */ async (context) => {
  const value = await fixture(context);
  assert.deepEqual(await installRelease(value), {
    version : '0.1.1',
    changed : true,
  });
  const manifest = path.join(value.root, 'current.json');
  assert.deepEqual(JSON.parse(await fs.readFile(manifest)), value.release);
  assert.deepEqual(
    await fs.readFile(path.join(value.root, '0.1.1', value.artifact.filename)),
    value.content,
  );
  assert.equal((await installRelease(value)).changed, false);
  await fs.unlink(manifest);
  assert.equal((await installRelease(value)).changed, true);
  value.release.notes = 'No se permite cambiar la misma versión';
  await fs.writeFile(path.join(value.input, 'current.json'), JSON.stringify(value.release));
  await assert.rejects(installRelease(value), /inmutable/);
  assert.equal(JSON.parse(await fs.readFile(manifest)).notes, '');
});
test('corrupción, enlaces, bloqueo y versión esperada distinta nunca publican', /** @param {Object} context Ciclo de vida. @returns {Promise<void>} Comprueba fallos antes del cambio del manifiesto. */ async (context) => {
  const value = await fixture(context);
  await assert.rejects(
    installRelease({
      ...value,
      expectedVersion : '0.1.0',
    }),
    /Cambió/,
  );
  await fs.mkdir(path.join(value.root, '.publish-lock'));
  await assert.rejects(installRelease(value), { code: 'EEXIST' });
  await fs.rmdir(path.join(value.root, '.publish-lock'));
  const filename = path.join(value.input, value.artifact.filename);
  await fs.writeFile(filename, Buffer.alloc(value.content.length));
  await assert.rejects(installRelease(value), /no coincide/);
  await fs.unlink(filename);
  await fs.writeFile(path.join(value.input, 'outside'), value.content);
  await fs.symlink(path.join(value.input, 'outside'), filename);
  await assert.rejects(installRelease(value), /no coincide/);
  assert.deepEqual(await fs.readdir(value.root), []);
});
