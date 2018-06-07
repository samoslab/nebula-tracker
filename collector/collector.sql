create table IF NOT EXISTS ACTION_LOG(
    TICKET STRING(64) NOT NULL PRIMARY KEY,
    TICKET_CLIENT_ID STRING(30) DEFAULT NULL,
    CLT_NODE_ID STRING(30) DEFAULT NULL,
    CLT_TYPE INT DEFAULT NULL,
    CLT_TIMESTAMP TIMESTAMPTZ DEFAULT NULL,
    CLT_SUCCESS BOOL DEFAULT NULL,
    CLT_FILE_HASH STRING(30) DEFAULT NULL,
    CLT_FILE_SIZE INT DEFAULT NULL,
    CLT_BLOCK_HASH STRING(30) DEFAULT NULL,
    CLT_BLOCK_SIZE INT DEFAULT NULL,
    CLT_BEGIN_TIME TIMESTAMPTZ DEFAULT NULL,
    CLT_END_TIME TIMESTAMPTZ DEFAULT NULL,
    CLT_TRANSPORT_SIZE INT DEFAULT NULL,
    CLT_ERROR_INFO STRING(255) DEFAULT NULL, 
    PARTITION_SEQ INT DEFAULT NULL,
    CHECKSUM BOOL DEFAULT NULL,
    BLOCK_SEQ INT DEFAULT NULL,
    PVD_NODE_ID STRING(30) DEFAULT NULL,
    PVD_TYPE INT DEFAULT NULL,
    PVD_TIMESTAMP TIMESTAMPTZ DEFAULT NULL,
    PVD_SUCCESS BOOL DEFAULT NULL, 
    PVD_FILE_HASH STRING(30) DEFAULT NULL,
    PVD_FILE_SIZE INT DEFAULT NULL,
    PVD_BLOCK_HASH STRING(30) DEFAULT NULL,
    PVD_BLOCK_SIZE INT DEFAULT NULL,
    PVD_BEGIN_TIME TIMESTAMPTZ DEFAULT NULL,
    PVD_END_TIME TIMESTAMPTZ DEFAULT NULL,
    PVD_TRANSPORT_SIZE INT DEFAULT NULL,
    PVD_ERROR_INFO STRING(255) DEFAULT NULL
);


create table IF NOT EXISTS CLIENT_PUB_KEY(
    NODE_ID STRING(30) NOT NULL PRIMARY KEY, 
    PUBLIC_KEY BYTES NOT NULL,
    CREATION TIMESTAMPTZ NOT NULL
);

create table IF NOT EXISTS PROVIDER_PUB_KEY(
    NODE_ID STRING(30) NOT NULL PRIMARY KEY, 
    PUBLIC_KEY BYTES NOT NULL，
    CREATION TIMESTAMPTZ NOT NULL
);