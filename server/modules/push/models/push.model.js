/** @file Instalaciones privadas y cola de entregas con unicidad, caducidad y estados explícitos. */
'use strict';
/**
 * Construye modelos sin abrir conexiones ni producir consultas.
 * @param {Object} mongoose Instancia de persistencia de la composición.
 * @returns {Object} Modelos de dispositivos y entregas del servicio.
 */
function createPushModels(mongoose) {
  const { Schema } = mongoose;
  const device     = new Schema(
    {
      _id         : String,
      bindingId   : {
        type     : String,
        required : true,
      },
      generation  : {
        type     : Number,
        required : true,
      },
      fid         : {
        type     : String,
        required : true,
      },
      platform    : {
        type     : String,
        enum     : ['ios', 'android'],
        required : true,
      },
      userCode    : {
        type     : String,
        required : true,
      },
      workspace   : {
        type     : String,
        required : true,
      },
      sessionCode : {
        type     : String,
        required : true,
      },
      updatedAt   : {
        type     : Date,
        required : true,
      },
      expiresAt   : {
        type     : Date,
        required : true,
      },
    },
    {
      versionKey : false,
      autoIndex  : false,
      autoCreate : false,
      collection : 'PLATFORM_PUSH_DEVICE',
    },
  );
  device.index({ fid: 1 }, { unique: true });
  device.index({
    workspace : 1,
    userCode  : 1,
  });
  device.index({ expiresAt: 1 }, { expireAfterSeconds: 0 });
  const delivery = new Schema(
    {
      _id              : String,
      installationId   : {
        type     : String,
        required : true,
      },
      bindingId        : {
        type     : String,
        required : true,
      },
      eventId          : {
        type     : String,
        required : true,
      },
      kind             : {
        type    : String,
        enum    : ['chat', 'customer-activity', 'ticket'],
        default : 'chat',
      },
      ticketCode       : String,
      folio            : String,
      eventLabel       : String,
      internal         : Boolean,
      conversationCode : String,
      sequence         : Number,
      notificationCode : String,
      activityCode     : String,
      reminderRevision : Number,
      userCode         : {
        type     : String,
        required : true,
      },
      workspace        : {
        type     : String,
        required : true,
      },
      state            : {
        type     : String,
        enum     : ['pending', 'checking', 'sending', 'sent', 'uncertain', 'discarded', 'failed'],
        required : true,
      },
      attempts         : {
        type    : Number,
        default : 0,
      },
      nextAt           : {
        type     : Date,
        required : true,
      },
      lease            : String,
      leaseUntil       : Date,
      validUntil       : {
        type     : Date,
        required : true,
      },
      expiresAt        : {
        type     : Date,
        required : true,
      },
      resultCode       : String,
    },
    {
      versionKey : false,
      autoIndex  : false,
      autoCreate : false,
      collection : 'PLATFORM_PUSH_DELIVERY',
    },
  );
  delivery.index({
    state      : 1,
    nextAt     : 1,
    leaseUntil : 1,
  });
  delivery.index({ expiresAt: 1 }, { expireAfterSeconds: 0 });
  return {
    devices    : mongoose.models.PushDevice || mongoose.model('PushDevice', device),
    deliveries : mongoose.models.PushDelivery || mongoose.model('PushDelivery', delivery),
  };
}
module.exports = { createPushModels };
