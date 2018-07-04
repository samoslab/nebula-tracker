create table IF NOT EXISTS FILE(
    ID UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    HASH STRING(30) NOT NULL,
    -- HASH STRING(30) NOT NULL PRIMARY KEY, 
    TYPE STRING(64),
    ENCRYPT_KEY BYTES,
    CREATION TIMESTAMPTZ NOT NULL,
    LAST_MODIFIED TIMESTAMPTZ NOT NULL,
    ACTIVE BOOL NOT NULL DEFAULT true,
    REMOVED BOOL NOT NULL DEFAULT false,
    SIZE INT NOT NULL,
    DATA BYTES DEFAULT NULL,
    REF_COUNT INT DEFAULT 1,
    PARTITION_COUNT INT DEFAULT 0,
    BLOCKS STRING[] DEFAULT NULL,
    DONE BOOL DEFAULT false,
    STORE_VOLUME INT DEFAULT 0,
    CREATOR_NODE_ID STRING(30) NOT NULL,
    SHARE BOOL DEFAULT true,
    PRIVATE BOOL DEFAULT false,
    INVALID BOOL DEFAULT false,
    INDEX FILE_HASH(HASH),
    UNIQUE (HASH,CREATOR_NODE_ID)
);

create table IF NOT EXISTS FILE_OWNER(
    ID UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    REMOVED BOOL NOT NULL DEFAULT false,
    CREATION TIMESTAMPTZ NOT NULL,
    LAST_MODIFIED TIMESTAMPTZ NOT NULL,
    NODE_ID STRING(30) NOT NULL REFERENCES CLIENT (NODE_ID),
    FOLDER BOOL NOT NULL DEFAULT false,
    NAME STRING(300) NOT NULL,
    SPACE_NO INT NOT NULL,
    TYPE STRING(64),
    PARENT_ID UUID DEFAULT NULL, 
    MOD_TIME TIMESTAMPTZ NOT NULL,
    HASH STRING(30) DEFAULT NULL,
    SIZE INT NOT NULL DEFAULT 0,
    INDEX FILE_OWNER_NAME(NAME),
    INDEX FILE_OWNER_PARENT_ID(PARENT_ID),
    INDEX FILE_OWNER_MOD_TIME(MOD_TIME),
    INDEX FILE_OWNER_SIZE(SIZE)
);
ALTER TABLE FILE_OWNER ADD CONSTRAINT PARENT_ID FOREIGN KEY (PARENT_ID) REFERENCES FILE_OWNER (ID);

create table IF NOT EXISTS FILE_VERSION(
    ID UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    CREATION TIMESTAMPTZ NOT NULL,
    OWNER_ID UUID NOT NULL REFERENCES FILE_OWNER (ID),
    NODE_ID STRING(30) NOT NULL REFERENCES CLIENT (NODE_ID),
    HASH STRING(30) NOT NULL,
    TYPE STRING(64),
    UNIQUE (OWNER_ID, HASH)
);



