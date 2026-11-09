/** @file Publica instaladores privados verificados mediante bloqueo, versiones inmutables y sustitución atómica del catálogo. */
'use strict';
const fs                              = require('node:fs/promises');
const { constants, createReadStream } = require('node:fs');
const { createHash, randomUUID }      = require('node:crypto');
const path                            = require('node:path');
const {
  validateRelease,
} = require('../server/modules/desktop-release/services/desktop-release.service');
const { desktopReleaseCatalog } = require('../server/adapters/files/desktop-release.catalog');

/** @param {string} filename Archivo regular sin enlaces. @returns {Promise<string>} Huella de los bytes completos. */
async function digest(filename) {
  const hash = createHash('sha256');
  for await (const chunk of createReadStream(filename, {
    flags : constants.O_RDONLY | constants.O_NOFOLLOW,
  }))
    hash.update(chunk);
  return hash.digest('hex');
}

/** @param {string} directory Directorio privado. @param {Object} release Catálogo validado. @returns {Promise<void>} Comprueba todos los bytes, sin aceptar enlaces ni inventarios parciales. */
async function verifyFiles(directory, release) {
  if ((await fs.realpath(directory)) !== directory)
    throw new Error('No se admiten enlaces en la distribución.');
  for (const artifact of release.artifacts) {
    const filename = path.join(directory, artifact.filename);
    const stat     = await fs.lstat(filename);
    if (
      !stat.isFile() ||
      stat.size !== artifact.bytes ||
      (await digest(filename)) !== artifact.sha256
    )
      throw new Error('El instalador no coincide con la publicación verificada.');
  }
}

/** @param {string} filename Archivo o directorio ya escrito. @returns {Promise<void>} Fuerza persistencia antes de anunciar la versión. */
async function syncPath(filename) {
  const handle = await fs.open(filename, 'r');
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

/**
 * @param {Object} options Origen preparado por CI, raíz preaprovisionada y versión anterior esperada (none si no existe).
 * @returns {Promise<Object>} Resultado idempotente; sólo current.json determina la versión anunciada.
 * @throws {Error} Conserva la versión vigente ante corrupción, publicación concurrente o versión inmutable distinta.
 */
async function installRelease({ input, root, expectedVersion }) {
  if (
    typeof expectedVersion !== 'string' ||
    !/^(none|(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*))$/.test(expectedVersion)
  )
    throw new Error('Indique explícitamente --expected-version VERSION o none.');
  const source      = path.resolve(input);
  const destination = path.resolve(root);
  if ((await fs.realpath(destination)) !== destination)
    throw new Error('La raíz debe existir y no contener enlaces.');
  const release = await desktopReleaseCatalog(source)();
  validateRelease(release);
  await verifyFiles(source, release);
  const serialized = JSON.stringify(release, null, 2) + '\n';
  const lock       = path.join(destination, '.publish-lock');
  await fs.mkdir(lock, { mode: 0o700 });
  let staging;
  let pendingManifest;
  try {
    await fs.writeFile(
      path.join(lock, 'owner.json'),
      JSON.stringify({
        pid       : process.pid,
        version   : release.version,
        startedAt : new Date().toISOString(),
      }),
      {
        flag : 'wx',
        mode : 0o600,
      },
    );
    const current = await desktopReleaseCatalog(destination)();
    if (current) validateRelease(current);
    const versionDirectory = path.join(destination, release.version);
    if (current?.version === release.version) {
      if (JSON.stringify(current) !== JSON.stringify(release))
        throw new Error('La versión ya publicada es inmutable.');
      await verifyFiles(versionDirectory, release);
      return {
        version : release.version,
        changed : false,
      };
    }
    if ((current?.version || 'none') !== expectedVersion)
      throw new Error('Cambió la versión vigente; revise antes de publicar.');
    try {
      await fs.lstat(versionDirectory);
      const installed = await desktopReleaseCatalog(versionDirectory)();
      if (JSON.stringify(installed) !== JSON.stringify(release))
        throw new Error('La versión existente es inmutable.');
      await verifyFiles(versionDirectory, release);
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
      staging = await fs.mkdtemp(path.join(destination, '.staging-'));
      await fs.chmod(staging, 0o750);
      for (const artifact of release.artifacts) {
        const filename = path.join(staging, artifact.filename);
        await fs.copyFile(path.join(source, artifact.filename), filename, constants.COPYFILE_EXCL);
        await fs.chmod(filename, 0o640);
        await syncPath(filename);
      }
      await verifyFiles(staging, release);
      const manifest = path.join(staging, 'current.json');
      await fs.writeFile(manifest, serialized, {
        flag : 'wx',
        mode : 0o640,
      });
      await syncPath(manifest);
      await syncPath(staging);
      await fs.rename(staging, versionDirectory);
      staging = undefined;
      await syncPath(destination);
    }
    pendingManifest = path.join(destination, `.current-${randomUUID()}.json`);
    await fs.writeFile(pendingManifest, serialized, {
      flag : 'wx',
      mode : 0o640,
    });
    await syncPath(pendingManifest);
    await fs.rename(pendingManifest, path.join(destination, 'current.json'));
    pendingManifest = undefined;
    await syncPath(destination);
    return {
      version : release.version,
      changed : true,
    };
  } finally {
    if (staging)
      await fs.rm(staging, {
        recursive : true,
        force     : true,
      });
    if (pendingManifest) await fs.rm(pendingManifest, { force: true });
    await fs.rm(lock, { recursive: true });
  }
}

/** @param {string[]} args Opciones administrativas. @returns {Object} Configuración explícita de publicación. */
function parseArguments(args) {
  const result = {};
  for (let index = 0; index < args.length; index += 2) {
    const name = {
      '--input'            : 'input',
      '--root'             : 'root',
      '--expected-version' : 'expectedVersion',
    }[args[index]];
    if (!name || !args[index + 1] || result[name])
      throw new Error('Uso: --input DIR --root DIR --expected-version VERSION|none');
    result[name] = args[index + 1];
  }
  if (!result.input || !result.root) throw new Error('Se requieren origen y raíz privados.');
  return result;
}
if (require.main === module)
  installRelease(parseArguments(process.argv.slice(2)))
    .then(
      /** @param {Object} result Publicación confirmada. @returns {void} Emite versión sin rutas privadas. */
      (result) => console.log(JSON.stringify(result)),
    )
    .catch(
      /** @param {Error} error Fallo administrativo. @returns {void} Devuelve fallo sin modificar el catálogo anterior. */
      (error) => {
        console.error(error.message);
        process.exitCode = 1;
      },
    );
module.exports = { installRelease };
