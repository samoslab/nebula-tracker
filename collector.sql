create table IF NOT EXISTS ACTION_LOG(
    TICKET STRING(40) NOT NULL PRIMARY KEY,
    CLIENT_NODE_ID STRING(30) DEFAULT NULL,
    CLIENT_TYPE INT DEFAULT NULL,
    CLIENT_TIMESTAMP TIMESTAMPTZ DEFAULT NULL,
    CLIENT_SUCCESS BOOL DEFAULT NULL,
    CLIENT_BLOCK_HASH STRING(30) DEFAULT NULL,
    CLIENT_BLOCK_SIZE INT DEFAULT NULL,
    CLIENT_BEGIN_TIME TIMESTAMPTZ DEFAULT NULL,
    CLIENT_END_TIME TIMESTAMPTZ DEFAULT NULL,
    CLIENT_TRANSPORT_SIZE INT DEFAULT NULL,
    CLIENT_INFO STRING(255) DEFAULT NULL, 
    FILE_HASH STRING(30) DEFAULT NULL,
    FILE_SIZE INT DEFAULT NULL,
    PARTITION_SEQ INT DEFAULT NULL,
    CHECKSUM BOOL DEFAULT NULL,
    BLOCK_SEQ INT DEFAULT NULL,
    PROVIDER_NODE_ID STRING(30) DEFAULT NULL,
    PROVIDER_TYPE INT DEFAULT NULL,
    PROVIDER_TIMESTAMP TIMESTAMPTZ DEFAULT NULL,
    PROVIDER_SUCCESS BOOL DEFAULT NULL, 
    PROVIDER_BLOCK_HASH STRING(30) DEFAULT NULL,
    PROVIDER_BLOCK_SIZE INT DEFAULT NULL,
    PROVIDER_BEGIN_TIME TIMESTAMPTZ DEFAULT NULL,
    PROVIDER_END_TIME TIMESTAMPTZ DEFAULT NULL,
    PROVIDER_TRANSPORT_SIZE INT DEFAULT NULL,
    PROVIDER_INFO STRING(255) DEFAULT NULL
);