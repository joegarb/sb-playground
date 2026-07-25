// Well-known emulator connection string. UseDevelopmentEmulator=true = plain AMQP, no TLS.
// In real Azure this comes from config / Key Vault and points at your namespace.
export const CONNECTION_STRING =
  "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";

export const TOPIC = "design-changes";
