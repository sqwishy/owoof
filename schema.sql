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
    ( id   integer primary key
    , uuid blob not null unique
    );

create table "attributes"
    -- TODO consider using autoincrement here if this is meant to track historical
    -- attributes?
    ( rowid         integer primary key
    , ident         text    not null
    -- one or many;
    -- , cardinality   integer not null default 0
    -- either; not unique, unique for entity-attribute, or unique for attribute ...
    -- , uniqueness    integer not null default 0
    );

create unique index "attributes_ident" on "attributes" ("ident");

create table "datoms"
    ( e     integer not null
    , a     integer not null -- references attributes(id)
    , v     blob    not null
    -- denormalized from attributes...
    , is_indexed boolean not null default false
    , is_unique  boolean not null default false
    -- , tx    integer not null -- references "db/transactions" (id)
    );

-- select * from sqlite_sequence;

create        index "datoms_eavt" on "datoms" (e, a, v);
create        index "datoms_aevt" on "datoms" (a, e, v);
-- datoms that are :db/unique or :db/index
create unique index "datoms_avet_index"  on "datoms" (a, v, e) where "datoms"."is_indexed";
create unique index "datoms_avet_unique" on "datoms" (a, v)    where "datoms"."is_unique";
-- datoms with attributes that are of the type :db.type/ref
-- create unique index "datoms_vaet" on "datom" (v, a, e);

--

-- insert into attributes (ident) values ("article/title");
-- delete from attributes where id = 1;
-- insert into attributes (ident) values ("article/content");
-- select * from attributes;
-- 
-- insert into datoms (e, a, v) values (1, 1, "hi");
-- 
-- select rowid, * from datoms;

-- .timer on
-- .headers on
-- 
-- explain query plan
--   select subject, op, object, tx
--     from "attr:person/favourite-food"
-- group by subject, op
-- order by tx desc, op desc;
