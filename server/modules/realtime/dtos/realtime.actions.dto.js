/** @file Contrato cerrado de lecturas HTTP correlacionadas con ventanas realtime. */
'use strict';
module.exports = require('../../../contracts/action-dto').createActionDtos([
  'realtime-read',
  'platform.resource.changed',
  'documents.repository.changed',
  'documents.settings.changed',
  'supplier.supplier.changed',
  'product.resource.changed',
  'customer.resource.changed',
  'directory.resource.changed',
  'documents-repository-read',
  'documents-repositories-read',
  'connect.meta-conversation.changed',
  'communication.chat.changed',
  'connect.mailbox.changed',
  'connect.mail-account.changed',
  'connect.settings.changed',
  'connect.template.changed',
  'connect.meta.changed',
  'security.access.changed',
  'security.resource.changed',
]);
