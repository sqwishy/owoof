-- create table "transactions"
--     ( id            integer primary key
--     , timestamp     integer not null
--     );

-- create table "history"
--     ( e     integer not null
--     , a     integer not null
--     , v     blob    not null
--     , tx    integer not null references "db/transactions" (id)
--     , op    integer not null
--     )

-- TODO autoincrement id to avoid re-use?
create table "entities"
    ( rowid     integer primary key
    , uuid      blob    not null
    );

create unique index "entities_uuid" on "entities" ("uuid");

create table "datoms"
    ( e     integer not null references entities(rowid)
    , a     integer not null references entities(rowid)
    , t     integer not null
    , v     blob    not null
    -- , is_indexed boolean not null default false
    -- , is_unique  boolean not null default false
    -- , tx    integer not null -- references "db/transactions" (id)
    );

-- thinkingface.jpg
-- TODO some attributes must be unique for the database ...? like :attr/ident?
create unique index "datoms_ea"  on "datoms" (e, a);

create        index "datoms_eav" on "datoms" (e, a, t, v);
create        index "datoms_aev" on "datoms" (a, e, t, v);
create        index "datoms_ave" on "datoms" (a, t, v, e);-- where "datoms"."is_indexed";
