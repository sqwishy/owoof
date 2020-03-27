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
    , uuid blob    not null unique
    );

create table "attributes"
    -- TODO consider using autoincrement here if this is meant to track historical
    -- attributes?
    ( rowid         integer primary key
    , ident         text    not null
    -- type?
    -- , t             blob    not null
    -- one or many;
    -- , cardinality   integer not null default 0
    -- either; not unique, unique for entity-attribute, or unique for attribute ...
    -- , uniqueness    integer not null default 0
    );

create unique index "attributes_ident" on "attributes" ("ident");

create table "datoms"
    ( e     integer not null references entities(id)
    , a     integer not null references attributes(id)
    , v     blob    not null
    -- the value type stored here ...
    -- the values of this are actually just either an entity reference or not that
    -- , t     blob    not null
    , is_ref boolean not null
    -- , is_indexed boolean not null default false
    -- , is_unique  boolean not null default false
    -- , tx    integer not null -- references "db/transactions" (id)
    );

-- select * from sqlite_sequence;

create        index "datoms_eav" on "datoms" (e, a, v);
create        index "datoms_aev" on "datoms" (a, e, v);
-- datoms that are :db/unique or :db/index
create        index "datoms_ave_index"  on "datoms" (a, v, e);-- where "datoms"."is_indexed";
-- create unique index "datoms_ave_unique" on "datoms" (a, v, e) where "datoms"."is_unique";
-- datoms with attributes that are of the type :db.type/ref
-- create unique index "datoms_vaet" on "datom" (v, a, e);

-- explain with "_s" as (select e from datoms where (a = ? AND v = ?) OR (a = ? AND v = ?))
--            , "_p" as (select v from datoms where e in "_s" AND a = ?)
--            , "_g" as (select v from datoms where e in "_p" AND a = ?)
--       select "v" from "_g";


-- c customer/name n
-- c customer/is_active true
-- o order/customer c
-- o order/date d
-- ????????
-- select "_n".v, "_d".v
--   from datoms "_n"
--   join datoms "_c" on "_n".e = "_c".v
--   join datoms "_c_is_active" on ???
--   join datoms "_d" on "_d".e = "_c".e
--   where "_n".a = 1
--     and "_c".a = 2
--     and "_d".a = 3


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
