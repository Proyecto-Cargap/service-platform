/** @file Selecciona identidades afectadas por una referencia durable y cerrada de Security. */
'use strict';
/** @param {Object} identity Principal acreditado del transporte o ventana. @param {Object} change Referencia validada del productor. @returns {boolean} La referencia puede modificar el acceso de este principal. */
function matchesAccess(identity, change) {
  if (!identity) return false;
  const context = identity.context || identity;
  switch (change.resource) {
    case 'user':
      return identity.userCode === change.code;
    case 'session':
      return identity.sessionCode === change.code;
    case 'group':
      return (
        identity.actorType === 'documents-delegated' ||
        context.groups?.includes(change.code) === true
      );
    case 'workspace':
      return (
        identity.workspace === change.code ||
        identity.activeWorkspace === change.code ||
        context.workspaces?.includes(change.code) === true ||
        change.code === 'allWorkspaces'
      );
    // Las políticas almacenan códigos de entidad; la identidad contiene permisos resueltos.
    case 'grant':
      return (identity.delegation || context.delegation)?.grantCode === change.code;
    case 'policy':
      return true;
    default:
      return false;
  }
}
module.exports = { matchesAccess };
