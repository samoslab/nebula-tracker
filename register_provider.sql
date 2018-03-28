create table IF NOT EXISTS PROVIDER
    NODE_ID STRING(27) NOT NULL PRIMARY KEY, 
    PUBLIC_KEY BYTES NOT NULL,
    BILL_EMAIL STRING(128) NOT NULL,
    EMAIL_VERIFIED BOOL DEFAULT false,
    ENCRYPT_KEY BYTES NOT NULL, 
    CREATION TIMESTAMP NOT NULL,
    LAST_MODIFIED TIMESTAMP NOT NULL,
    RANDOM_CODE STRING(8) DEFAULT NULL,
    SEND_TIME TIMESTAMP DEFAULT NULL,
    ACTIVE BOOL NOT NULL DEFAULT true,
    REMOVED BOOL NOT NULL DEFAULT false
);