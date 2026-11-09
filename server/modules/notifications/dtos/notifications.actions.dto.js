/** @file DTO de entrada y salida de las acciones de notifications; reutilizan los esquemas vigentes del servicio. */
'use strict';
const { createActionDtos }    = require('../../../contracts/action-dto');
const notificationsActionDtos = createActionDtos([
  'billing.issuer.changed',
  'intelligence.run.changed',
  'intelligence.resource.changed',
  'billing.document.changed',
  'billing.operation.changed',
  'billing.audit.changed',
  'billing.expense-category.changed',
  'billing.cost-center.changed',
  'billing.pac.changed',
  'billing.sale-extraction.changed',
  'billing.sale.changed',
  'billing.expense.changed',
  'billing.expense-payment.changed',
  'billing.receipt.changed',
  'customer.activity.reminder-due',
  'notifications-center-personal-list',
  'notifications-center-personal-one',
  'notifications-center-personal-sender-picture',
  'notifications-center-personal-count',
  'notifications-center-personal-view',
  'notifications-center-personal-view-all',
  'notifications-center-personal-delete',
  'notifications-center-personal-delete-viewed',
  'notifications-center-admin-list',
  'notifications-center-admin-one',
  'notifications-center-admin-delete',
  'notifications-center-preview',
  'notifications-center-confirm',
  'notifications-center-broadcasts',
  'notifications-center-status',
  'notifications-center-retry',

  'notifications-create',
]);
module.exports = notificationsActionDtos;
