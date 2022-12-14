-- VALID_DAYS must be 30
create table IF NOT EXISTS PACKAGE(
    ID SERIAL PRIMARY KEY,
    NAME STRING(30) NOT NULL,
    -- LEVEL INT DEFAULT 0,
    PRICE INT NOT NULL,
    CREATION TIMESTAMPTZ NOT NULL,
    LAST_MODIFIED TIMESTAMPTZ NOT NULL,
    REMOVED BOOL NOT NULL DEFAULT false,
    VOLUME INT NOT NULL,
    NETFLOW INT NOT NULL,
    UP_NETFLOW INT NOT NULL,
    DOWN_NETFLOW INT NOT NULL,
    VALID_DAYS INT NOT NULL,
    REMARK STRING(255)    
);

insert into PACKAGE(NAME,PRICE,CREATION,LAST_MODIFIED,VOLUME,NETFLOW,UP_NETFLOW,DOWN_NETFLOW,VALID_DAYS) values('basic package',15000000,now(),now(),1024,6144,3072,3072,30);
insert into PACKAGE(NAME,PRICE,CREATION,LAST_MODIFIED,VOLUME,NETFLOW,UP_NETFLOW,DOWN_NETFLOW,VALID_DAYS) values('professional package',40000000,now(),now(),3072,18432,9216,9216,30);

create table IF NOT EXISTS PACKAGE_DISCOUNT(
    ID SERIAL PRIMARY KEY,
    PACKAGE_ID INT NOT NULL REFERENCES PACKAGE(ID),
    QUANTITY INT NOT NULL,
    DISCOUNT DECIMAL NOT NULL,
    UNIQUE (PACKAGE_ID, QUANTITY)
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
    UPGRADED BOOL NOT NULL DEFAULT false,
    DISCOUNT DECIMAL NOT NULL,
    VOLUME INT NOT NULL,
    NETFLOW INT NOT NULL,
    UP_NETFLOW INT NOT NULL,
    DOWN_NETFLOW INT NOT NULL,
    VALID_DAYS INT NOT NULL,
    START_TIME TIMESTAMPTZ DEFAULT NULL,
    END_TIME TIMESTAMPTZ DEFAULT NULL,
    PAY_TIME TIMESTAMPTZ DEFAULT NULL,
    REMARK STRING(255)  
);


create table IF NOT EXISTS AVAILABLE_ADDRESS(
    ADDRESS STRING(255) PRIMARY KEY,
    CHECKSUM STRING(255) NOT null,
    CREATION TIMESTAMPTZ NOT NULL,
    USED BOOL NOT null DEFAULT false,
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

