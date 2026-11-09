/** @file Recibos breves del productor compartidos por instancias, sin credenciales ni datos aportados como base por el cliente. */
'use strict';
/** @param {Object} connection Conexión Mongo del servicio. @returns {Object} Persistencia de lecturas autorizadas con vencimiento. */
function readReceipts(connection) {
  const collection = connection.db.collection('realtime_read_receipts');
  return {
    /** @returns {Promise<void>} Exige la migración explícita del índice TTL; no administra índices al arrancar. */
    async verify() {
      const indexes = await collection.listIndexes().toArray();
      if (
        !indexes.some(
          /** @param {Object} index Índice instalado. @returns {boolean} Vencimiento puntual de recibos. */
          (index) =>
            index.expireAfterSeconds === 0 &&
            index.key.expiresAt === 1 &&
            Object.keys(index.key).length === 1,
        )
      )
        throw new Error('REALTIME_RECEIPT_INDEX_REQUIRED');
    },
    /** @param {Object} receipt Resultado autorizado. @returns {Promise<void>} Conserva la base exacta emitida por HTTP. */
    async put(receipt) {
      await collection.insertOne({
        _id : receipt.receiptId,
        ...receipt,
      });
    },
    /** @param {Object} receipt Base obtenida por HTTP después del registro. @returns {Promise<boolean>} Una sola lectura completa la reserva; un cliente no puede sustituir datos ni anticipar la suscripción. */
    async complete(receipt) {
      const result = await collection.updateOne(
        {
          _id   : receipt.receiptId,
          owner : receipt.owner,
          state : 'registered',
        },
        {
          $set : {
            ...receipt,
            state : 'ready',
          },
        },
      );
      return result.modifiedCount === 1;
    },
    /** @param {Object} receipt Página anterior acreditada. @param {Object} progress Cursor y revisión emitidos por Documents. @returns {Promise<boolean>} Avanza una sola vez; carreras y respuestas tardías no sustituyen el progreso. */
    async advanceRepository(receipt, progress) {
      const result = await collection.updateOne(
        {
          _id        : receipt.receiptId,
          owner      : receipt.owner,
          state      : 'registered',
          nextCursor : receipt.nextCursor,
          count      : receipt.count,
          revision   : receipt.revision,
        },
        { $set: progress },
      );
      return result.modifiedCount === 1;
    },
    /** @param {string} receiptId Identificador opaco. @param {string} owner Huella del principal acreditado. @returns {Promise<Object|null>} Sólo el propietario recupera su base. */
    get(receiptId, owner) {
      return collection.findOne({
        _id : receiptId,
        owner,
      });
    },
  };
}
module.exports = { readReceipts };
