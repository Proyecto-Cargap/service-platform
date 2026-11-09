/** @file Lectura puntual del catálogo privado de instaladores; la publicación sustituye current.json atómicamente. */
'use strict';
const { open, realpath }   = require('node:fs/promises');
const { constants }        = require('node:fs');
const path                 = require('node:path');
const { ApplicationError } = require('../../shared/application-error');

/** @param {string} root Directorio privado administrado fuera de las releases de servicios. @returns {Function} Lee sólo al solicitar el catálogo. */
function desktopReleaseCatalog(root) {
  const directory = path.resolve(root);
  /** @returns {Promise<Object|null>} Manifiesto íntegro; null únicamente cuando no se ha publicado. */
  return async function readCurrent() {
    let handle;
    try {
      const canonical = await realpath(directory);
      if (canonical !== directory) throw new Error('DIRECTORY_SYMLINK');
      handle = await open(
        path.join(directory, 'current.json'),
        constants.O_RDONLY | constants.O_NOFOLLOW,
      );
      const stat = await handle.stat();
      if (!stat.isFile() || stat.size > 65536) throw new Error('INVALID_MANIFEST_FILE');
      return JSON.parse(await handle.readFile('utf8'));
    } catch (error) {
      if (error.code === 'ENOENT') return null;
      throw new ApplicationError(503, 'DESKTOP_RELEASE_UNAVAILABLE');
    } finally {
      if (handle) await handle.close();
    }
  };
}
module.exports = { desktopReleaseCatalog };
