/** @file Registro cercado por generación y cola durable; los resultados del proveedor no se almacenan. */
'use strict';
const { ApplicationError } = require('../../../shared/application-error');
/**
 * Compone consultas y escrituras de push con modelos, reloj e identificadores explícitos.
 * @param {Object} dependencies Modelos Mongoose, reloj y generador de leases.
 * @returns {Object} Puerto de registro y cola; los fallos técnicos se propagan.
 */
function createPushRepository({ devices, deliveries, now, newId }) {
  let indexes;
  /** Verifica índices antes de admitir registros o trabajos.
   * @returns {Promise<void>} Confirma los índices necesarios de ambos modelos.
   */
  async function ready() {
    if (!indexes) {
      const manifest = require('../../../mongodb-indexes.json').collections;
      indexes = require('../../../library/messaging/indexes')
        .verifyIndexes(devices.db.db, {
          PLATFORM_PUSH_DEVICE   : manifest.PLATFORM_PUSH_DEVICE,
          PLATFORM_PUSH_DELIVERY : manifest.PLATFORM_PUSH_DELIVERY,
        })
        .catch(
          /** Permite reintentar tras preparar los índices administrativamente. @param {Error} error Fallo de prerrequisitos. @returns {never} Propaga sin modificar datos. */ (
            error,
          ) => {
            indexes = undefined;
            throw error;
          },
        );
    }
    await indexes;
  }
  return {
    ready,
    /** @returns {Promise<number|undefined>} Próximo pendiente o lease recuperable; nunca incluye envíos inciertos. */
    async nextAt() {
      await ready();
      const pending  = await deliveries
        .findOne({ state: 'pending' })
        .sort({ nextAt: 1 })
        .select('nextAt')
        .lean();
      const checking = await deliveries
        .findOne({ state: 'checking' })
        .sort({ leaseUntil: 1 })
        .select('leaseUntil')
        .lean();
      const deadline = Math.min(
        pending ? new Date(pending.nextAt).getTime() : Infinity,
        checking ? new Date(checking.leaseUntil).getTime() : Infinity,
      );
      return Number.isFinite(deadline) ? deadline : undefined;
    },
    /**
     * Sustituye la instalación sólo con una generación posterior o una repetición idéntica.
     * @param {Object} value Registro acreditado, sin opciones aportadas por el cliente.
     * @returns {Promise<void>} Confirma la revisión; 409 impide restaurar contextos antiguos.
     */
    async register(value) {
      await ready();
      const { installationId, ...data } = value;
      try {
        await devices
          .findOneAndUpdate(
            {
              _id : installationId,
              $or : [
                { generation: { $lt: value.generation } },
                {
                  generation  : value.generation,
                  bindingId   : value.bindingId,
                  fid         : value.fid,
                  userCode    : value.userCode,
                  sessionCode : value.sessionCode,
                  workspace   : value.workspace,
                },
              ],
            },
            {
              $set : {
                ...data,
                expiresAt : new Date(value.updatedAt.getTime() + 30 * 86400000),
              },
            },
            {
              upsert : true,
              new    : true,
            },
          )
          .lean();
      } catch (error) {
        if (error.code === 11000) throw new ApplicationError(409, 'PUSH_REGISTRATION_CONFLICT');
        throw error;
      }
    },
    /**
     * Crea una entrega única por evento e instalación; procesa dispositivos por cursor acotado.
     * @param {Object} event Evento de Communication sin cuerpo del mensaje.
     * @returns {Promise<void>} Finaliza cuando todas las instalaciones elegibles están en la cola.
     */
    async enqueue(event) {
      await ready();
      const date       = now();
      const validUntil = new Date(new Date(event.creationDate).getTime() + 3600000);
      if (validUntil <= date) return;
      const cursor = devices
        .find({
          userCode  : { $in: event.recipientUserCodes },
          workspace : event.workspace,
          expiresAt : { $gt: date },
        })
        .lean()
        .cursor({ batchSize: 100 });
      for await (const device of cursor) {
        const id = `${event.messageCode}:${device._id}`;
        await deliveries.updateOne(
          { _id: id },
          {
            $setOnInsert : {
              installationId   : device._id,
              bindingId        : device.bindingId,
              eventId          : event.messageCode,
              conversationCode : event.conversationCode,
              sequence         : event.sequence,
              userCode         : device.userCode,
              workspace        : event.workspace,
              state            : 'pending',
              attempts         : 0,
              nextAt           : date,
              validUntil,
              expiresAt        : new Date(date.getTime() + 7 * 86400000),
            },
          },
          { upsert: true },
        );
      }
    },
    /**
     * Reclama un trabajo; una entrega que llegó a sending nunca se reenvía automáticamente.
     * @returns {Promise<Object|null>} Trabajo cercado o ausencia real de pendientes.
     */
    async claim() {
      await ready();
      const date = now();
      return deliveries
        .findOneAndUpdate(
          {
            $or : [
              {
                state  : 'pending',
                nextAt : { $lte: date },
              },
              {
                state      : 'checking',
                leaseUntil : { $lte: date },
              },
            ],
          },
          {
            $set : {
              state      : 'checking',
              lease      : newId(),
              leaseUntil : new Date(date.getTime() + 120000),
            },
            $inc : { attempts: 1 },
          },
          {
            new  : true,
            sort : { nextAt: 1 },
          },
        )
        .lean();
    },
    /** Lee exclusivamente la vinculación que originó la entrega.
     * @param {Object} job Trabajo reclamado.
     * @returns {Promise<Object|null>} Dispositivo vigente o ausencia por revocación/caducidad.
     */
    async device(job) {
      return devices
        .findOne({
          _id       : job.installationId,
          bindingId : job.bindingId,
          userCode  : job.userCode,
          workspace : job.workspace,
          expiresAt : { $gt: now() },
        })
        .lean();
    },
    /** Cerca el inicio de una operación externa para no duplicarla después de un cierre inesperado.
     * @param {Object} job Trabajo reclamado.
     * @returns {Promise<boolean>} La propiedad del lease sigue vigente.
     */
    async beginSending(job) {
      const result = await deliveries.updateOne(
        {
          _id        : job._id,
          lease      : job.lease,
          state      : 'checking',
          leaseUntil : { $gt: now() },
        },
        { $set: { state: 'sending' } },
      );
      return result.modifiedCount === 1;
    },
    /** Finaliza con código seguro; las respuestas del SDK y las direcciones nunca se registran.
     * @param {Object} job Trabajo cercado.
     * @param {string} state Estado final o pending para un fallo inequívocamente recuperable.
     * @param {string} code Código de resultado público sin datos del proveedor.
     * @returns {Promise<void>} Confirma el resultado si aún pertenece al lease.
     */
    async finish(job, state, code) {
      await deliveries.updateOne(
        {
          _id   : job._id,
          lease : job.lease,
        },
        {
          $set : {
            state,
            resultCode : code,
            nextAt     : new Date(now().getTime() + Math.min(600000, 30000 * job.attempts)),
          },
        },
      );
    },
    /** Retira únicamente una vinculación cuya dirección dejó de existir en Firebase.
     * @param {Object} job Trabajo del destinatario invalidado.
     * @returns {Promise<void>} La eliminación no afecta un registro posterior.
     */
    async invalidate(job) {
      await devices.deleteOne({
        _id       : job.installationId,
        bindingId : job.bindingId,
      });
    },
  };
}
module.exports = { createPushRepository };
