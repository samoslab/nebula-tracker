create table IF NOT EXISTS PACKAGE(
    ID SERIAL PRIMARY KEY,
    NAME STRING(30) NOT NULL,
    LEVEL INT DEFAULT 0,
    PRICE INT NOT NULL,
    CREATION TIMESTAMPTZ NOT NULL,
    LAST_MODIFIED TIMESTAMPTZ NOT NULL,
    REMOVED BOOL NOT NULL DEFAULT false,
    VOLUME INT NOT NULL,
    NETFLOW INT DEFAULT NULL,
    UP_NETFLOW INT DEFAULT NULL,
    DOWN_NETFLOW INT DEFAULT NULL,
    VALID_DAYS INT NOT NULL,
    REMARK STRING(255)    
);

create table IF NOT EXISTS CLIENT_ORDER(
    ID UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    REMOVED BOOL NOT NULL DEFAULT false,
    CREATION TIMESTAMPTZ NOT NULL,
    LAST_MODIFIED TIMESTAMPTZ NOT NULL,
    NODE_ID STRING(30) NOT NULL REFERENCES CLIENT (NODE_ID),
    PACKAGE_ID INT NOT NULL REFERENCES PACKAGE(ID),
    QUANTITY INT NOT NULL,
    TOTAL_AMOUNT INT NOT NULL,
    UPGRADED BOOL NOT DEFAULT false,
    DISCOUNT FLOAT NOT NULL,
    VOLUME INT NOT NULL,
    NETFLOW INT DEFAULT NULL,
    UP_NETFLOW INT DEFAULT NULL,
    DOWN_NETFLOW INT DEFAULT NULL,
    VALID_DAYS INT NOT NULL,
    START_TIME TIMESTAMPTZ DEFAULT NULL,
    END_TIME TIMESTAMPTZ DEFAULT NULL,
    PAY_TIME TIMESTAMPTZ DEFAULT NULL,
    REMARK STRING(255)  
);


create table IF NOT EXISTS AVAILABLE_ADDRESS(
    ADDRESS STRING(255) PRIMARY KEY,
    CHECKSUM STRING(255) DEFAULT null,
    CREATION TIMESTAMPTZ NOT NULL,
    USED BOOL DEFAULT false,
    USAGE_TIME TIMESTAMPTZ DEFAULT NULL
);

create table IF NOT EXISTS DEPOSIT_RECORD(
    ID SERIAL PRIMARY KEY,
    CREATION TIMESTAMPTZ NOT NULL,
    ADDRESS varchar(255) not null,
    SEQ bigint not null unique,
    TRANSACTION_TIME bigint not null,
    TRANSACTION_ID varchar(512) not null,
    AMOUNT bigint not null,
    HEIGHT bigint not null,
    UNIQUE (ADDRESS, TRANSACTION_ID,AMOUNT,HEIGHT)
);

