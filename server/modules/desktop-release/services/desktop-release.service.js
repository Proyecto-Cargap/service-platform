/** @file Consulta de instaladores publicados; exige sesión y excluye paquetes sin firma verificada. */
'use strict';
const { ApplicationError } = require('../../../shared/application-error');
const { validate }         = require('../../../contracts/action-dto');
const manifest             = require('../../../contracts/service.json');
const releaseSchema        = manifest.endpoints.find(
  /** @param {Object} entry Contrato registrado. @returns {boolean} Identifica la lectura del catálogo. */
  (entry) => entry.name === 'desktop-release-current',
).successData.properties.release.anyOf[1];

/** @param {Object} release Catálogo candidato. @returns {void} Rechaza metadatos incompatibles antes de publicar o servir. */
function validateRelease(release) {
  if (!validate(release, releaseSchema, false))
    throw new ApplicationError(503, 'DESKTOP_RELEASE_INVALID');
  const targets = new Set();
  for (const artifact of release.artifacts) {
    if (
      targets.has(artifact.target) ||
      artifact.filename !==
        `Xidden-Documentos-${release.version}-${artifact.target}.${artifact.target === 'win-x64' ? 'exe' : 'dmg'}` ||
      artifact.signature !==
        (artifact.target === 'win-x64' ? 'authenticode' : 'developer-id-notarized')
    )
      throw new ApplicationError(503, 'DESKTOP_RELEASE_INVALID');
    targets.add(artifact.target);
  }
  if (!Number.isFinite(Date.parse(release.publishedAt)))
    throw new ApplicationError(503, 'DESKTOP_RELEASE_INVALID');
}

/** @param {Object} dependencies Lector privado y contexto autenticado. @returns {Object} Consultas sin escrituras ni sondeo. */
function desktopReleaseService({ readCurrent, getContext }) {
  if (typeof readCurrent !== 'function' || typeof getContext !== 'function')
    throw new TypeError('Se requieren catálogo y contexto.');
  /** @returns {Promise<Object|null>} Versión acreditada o ausencia; los fallos no se convierten en ausencia. */
  async function current() {
    const context = getContext();
    if (!context?.user?.userCode || !context.sessionCode || context.sessionCode === 'no session')
      throw new ApplicationError(401, 'UNAUTHORIZED');
    const release = await readCurrent();
    if (release === null) return null;
    validateRelease(release);
    return release;
  }
  return {
    current,
    /** @param {Object} data Versión e instalador elegidos. @returns {Promise<Object>} Artefacto vigente autorizado. */
    async download(data) {
      const release = await current();
      if (!release) throw new ApplicationError(404, 'DESKTOP_RELEASE_UNAVAILABLE');
      if (release.version !== data.version)
        throw new ApplicationError(409, 'DESKTOP_RELEASE_CHANGED');
      const artifact = release.artifacts.find(
        /** @param {Object} candidate Instalador publicado. @returns {boolean} Coincidencia del identificador permitido. */
        (candidate) => candidate.target === data.target,
      );
      if (!artifact) throw new ApplicationError(404, 'DESKTOP_ARTIFACT_UNAVAILABLE');
      return {
        version : release.version,
        artifact,
      };
    },
  };
}
module.exports = {
  desktopReleaseService,
  validateRelease,
};
