/** @file Correlación de manifiestos paginados por Gateway con una suscripción de repositorio, sin guardar listas del cliente. */
'use strict';
const { ApplicationError } = require('../../../shared/application-error');
const { readSnapshot }     = require('../../../library/messaging/read-receipt');
/** @param {Object} options Recibos compartidos, productor y primitivas técnicas. @returns {Object} Registro, lectura paginada y confirmación autorizados. */
function repositoryReads({ receipts, messaging, clock, identifier, hash, observe }) {
  /** @param {Object} identity Identidad del transporte. @returns {string} Vínculo con usuario, espacio y sesión o concesión; nunca con el token. */
  function owner(identity) {
    const context = identity.context;
    const scope   = context?.delegation
      ? ['delegation', context.delegation.grantCode, context.delegation.clientId]
      : ['session', context?.sessionCode];
    if (
      !context?.user?.userCode ||
      !context.workspace ||
      scope.some(
        /** @param {string} value Identificador acreditado. @returns {boolean} Falta una frontera de autorización. */ (
          value,
        ) => typeof value !== 'string' || !value,
      )
    )
      throw new ApplicationError(401, 'UNAUTHORIZED');
    return hash([context.user.userCode, context.workspace, ...scope]);
  }
  /** @param {Object} request Control HTTP o socket. @param {Object} identity Principal. @returns {Promise<Object>} Reserva exclusiva de esa identidad y generación. */
  async function registered(request, identity) {
    const receipt = await receipts.get(request.receiptId, owner(identity));
    if (
      !receipt ||
      receipt.expiresAt.getTime() <= clock() ||
      receipt.topic !== 'documents.entries' ||
      receipt.section !== 'repository' ||
      receipt.subscriptionId !== request.subscriptionId ||
      receipt.watchVersion !== request.watchVersion ||
      receipt.query.repositoryId !== request.query?.repositoryId
    )
      throw new ApplicationError(409, 'DOCUMENT_READ_DISCONTINUITY');
    return receipt;
  }
  return {
    /** @param {Object} request Consulta registrada. @param {Object} identity Principal acreditado. @returns {Promise<Object>} ACK exclusivamente técnico. */
    async register(request, identity) {
      const receipt = {
        receiptId        : identifier(),
        owner            : owner(identity),
        subscriptionId   : request.subscriptionId,
        watchVersion     : request.watchVersion,
        topic            : 'documents.entries',
        section          : 'repository',
        query            : { repositoryId: request.repositoryId },
        state            : 'registered',
        queryFingerprint : hash({ repositoryId: request.repositoryId }),
        nextCursor       : null,
        revision         : null,
        count            : 0,
        purpose          : request.type === 'resync' ? 'recovery' : 'explicit',
        expiresAt        : new Date(clock() + 120000),
      };
      await receipts.put(receipt);
      return {
        receiptId        : receipt.receiptId,
        queryFingerprint : receipt.queryFingerprint,
      };
    },
    /** @param {Object} request Página por Gateway. @param {Object} identity Principal de HTTP. @returns {Promise<Object>} Página y control del productor; ninguna lista se devuelve por socket. */
    async read(request, identity) {
      const receipt = await registered(request, identity);
      if (receipt.state !== 'registered' || receipt.nextCursor !== (request.pageCursor || null))
        throw new ApplicationError(409, 'DOCUMENT_READ_DISCONTINUITY');
      observe?.(receipt.purpose === 'recovery' ? 'http-recovery' : 'http-read');
      const response = await messaging.request(
        'service-documents',
        'documents-repository-manifest-get',
        {
          repositoryId : receipt.query.repositoryId,
          ...(receipt.nextCursor ? { cursor: receipt.nextCursor } : {}),
        },
        { context: identity.context },
      );
      if (response.statusCode !== 200)
        throw new ApplicationError(
          response.statusCode || 503,
          response.descriptionCode || 'DOCUMENT_READ_UNAVAILABLE',
        );
      const { data, cursor } = readSnapshot(response);
      if (
        data.repository?.id !== receipt.query.repositoryId ||
        !Number.isSafeInteger(data.revision) ||
        !Array.isArray(data.items) ||
        data.items.length > 500 ||
        data.notModified ||
        (receipt.revision !== null && receipt.revision !== data.revision) ||
        (data.nextCursor !== null &&
          (typeof data.nextCursor !== 'string' ||
            !data.nextCursor ||
            data.nextCursor === receipt.nextCursor))
      )
        throw new ApplicationError(409, 'DOCUMENT_READ_DISCONTINUITY');
      const count = receipt.count + data.items.length;
      if (count > 20000) throw new ApplicationError(413, 'DOCUMENT_MANIFEST_LIMIT');
      if (
        !(await receipts.advanceRepository(receipt, {
          revision   : data.revision,
          count,
          cursor,
          nextCursor : data.nextCursor,
          state      : data.nextCursor ? 'registered' : 'ready',
          expiresAt  : new Date(clock() + 120000),
        }))
      )
        throw new ApplicationError(409, 'DOCUMENT_READ_DISCONTINUITY');
      return {
        data,
        control : {
          subscriptionId   : receipt.subscriptionId,
          watchVersion     : receipt.watchVersion,
          topic            : receipt.topic,
          section          : receipt.section,
          queryFingerprint : receipt.queryFingerprint,
          receiptId        : receipt.receiptId,
          cursor,
        },
      };
    },
    /** @param {Object} request Confirmación sin registros aportados por el cliente. @param {Object} identity Principal vigente. @returns {Promise<Object>} Cursor de repositorio acreditado por todas las páginas HTTP. */
    async bind(request, identity) {
      const receipt = await registered(request, identity);
      if (receipt.state !== 'ready') throw new ApplicationError(409, 'DOCUMENT_READ_DISCONTINUITY');
      return {
        revision         : receipt.revision,
        cursor           : receipt.cursor,
        queryFingerprint : receipt.queryFingerprint,
      };
    },
  };
}
module.exports = { repositoryReads };
