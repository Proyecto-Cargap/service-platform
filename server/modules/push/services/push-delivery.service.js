/** @file Revalida cada entrega y cerca el efecto externo; un resultado incierto no se reenvía solo. */
'use strict';
/**
 * Compone una vuelta de entrega con puertos mínimos, sin SDK ni temporizadores.
 * @param {Object} dependencies Cola, acceso vigente, emisor y reloj.
 * @returns {Object} Operación tick verificable sin servicios reales.
 */
function createPushDelivery({ repository, eligible, sender, now }) {
  return {
    /** Procesa un destinatario; conserva resultados inciertos sin duplicar envíos.
     * @returns {Promise<boolean>} Había un trabajo que procesar.
     */
    async tick() {
      const job = await repository.claim();
      if (!job) return false;
      let sending = false;
      try {
        const device = await repository.device(job);
        if (!device || job.validUntil <= now() || !(await eligible(device, job))) {
          await repository.finish(job, 'discarded', 'ACCESS_OR_MESSAGE_EXPIRED');
          return true;
        }
        // Una renovación entre la consulta de permisos y el envío invalida este destinatario.
        const current = await repository.device(job);
        if (!current || current.fid !== device.fid) {
          await repository.finish(job, 'discarded', 'DEVICE_REPLACED');
          return true;
        }
        if (!(await repository.beginSending(job))) return true;
        sending = true;
        await sender.send(device, job);
        await repository.finish(job, 'sent', 'ACCEPTED');
      } catch (error) {
        if (error.code === 'PUSH_ADDRESS_INVALID') {
          await repository.invalidate(job);
          await repository.finish(job, 'discarded', error.code);
        } else if (!sending || error.retryable === true) {
          await repository.finish(
            job,
            job.attempts < 5 ? 'pending' : 'failed',
            'DELIVERY_UNAVAILABLE',
          );
        } else {
          await repository.finish(job, 'uncertain', 'DELIVERY_UNCERTAIN');
        }
      }
      return true;
    },
  };
}
module.exports = { createPushDelivery };
