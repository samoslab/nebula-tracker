create table IF NOT EXISTS TASK(
    ID UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    CREATION TIMESTAMPTZ NOT NULL,
    EXPIRE_TIME TIMESTAMPTZ DEFAULT NULL,
    PROVIDER_ID STRING(30) NOT NULL REFERENCES PROVIDER (NODE_ID),
    -- REPLICATE SEND REMOVE PROVE 
    TYPE STRING NOT NULL,
    FILE_ID UUID NOT NULL REFERENCES FILE (ID),
    FILE_HASH STRING(30) NOT NULL,
    FILE_SIZE INT NOT NULL,
    BLOCK_HASH STRING(30) NOT NULL,
    BLOCK_SIZE INT NOT NULL,
    OPPOSITE_ID STRING[],
    PROOF_ID UUID DEFAULT NULL REFERENCES PROOF_RECORD (ID),
    FINISHED boolean NOT NULL default false,
    FINISHED_TIME TIMESTAMPTZ,
    SUCCESS boolean default false,
    REMARK STRING(255),
    INDEX TASK_HASH(BLOCK_HASH)
);

create table IF NOT EXISTS PROOF_RECORD(
    ID UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    CREATION TIMESTAMPTZ NOT NULL,
    FILE_ID UUID NOT NULL REFERENCES FILE (ID),
    BLOCK_HASH STRING(30) NOT NULL,
    BLOCK_SIZE INT NOT NULL,
    CHUNK_SEQ int[],
    RANDOM_NUM BYTES[],
    PROVE_RESULT BYTES,
    FINISHED boolean NOT NULL default false,
    FINISHED_TIME TIMESTAMPTZ,
    PASS boolean default false,
    REMARK STRING(255),
    INDEX PROOF_RECORD_HASH(BLOCK_HASH)
);