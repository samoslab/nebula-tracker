PDP:
insert into TASK(CREATION,EXPIRE_TIME,PROVIDER_ID,TYPE,FILE_ID,FILE_HASH,FILE_SIZE,BLOCK_HASH,BLOCK_SIZE) select now(), now()+(INTERVAL '3day'), b.PROVIDER_ID,'PROVE',b.FILE_ID,f.HASH,f.SIZE,b.HASH,b.SIZE
from BLOCK b,PROOF_METADATA m, FILE f where b.FILE_ID=m.FILE_ID and b.HASH=m.HASH and b.FILE_ID=f.ID;


backup to private node:


recovery: