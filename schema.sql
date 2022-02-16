-- pragma foreign_keys=on;
-- begin;
-- FYI: "At this time SQLite supports only FOR EACH ROW triggers"


--------------------------------------------------------------------------------

create table "soup"
    ( rowid  integer primary key
    , t      integer not null
    , v      blob    not null
    , rc     integer not null default 0
    );

-- I guess t values are decided at compile time? So we can do this fairly reliably?
-- This seems to improve query time a bit but insert performance is much slower so idk.
create unique index "soup-tv-0" on "soup" (v) where t = 0;
create unique index "soup-tv-1" on "soup" (v) where t = 1;
create unique index "soup-tv-2" on "soup" (v) where t = 2;

create trigger "soup/no-updates" before update
            on "soup" when new.t != old.t or new.v != old.v
begin select raise (abort, 'not yet implemented (also confusing)');
end;

create trigger "soup/forget-zero-rc" before update
            on "soup" when new.rc = 0
begin delete from "soup" where rowid = new.rowid;
end;

--------------------------------------------------------------------------------

create table "triples"
    ( e  integer not null  references "entities" (rowid)
    , a  integer not null  references "attributes" (rowid)
    , v  integer not null  references "soup" (rowid)
    , primary key (e, a, v)
    ) without rowid;  -- <_<

-- Created programatically by the owoof library.
--- create index "triples-ave-N"  on "triples" (v, e) where a =  N;

-- TODO this kills the following query...
---     owoof  '?calvin :book/title "The Complete Calvin and Hobbes"' \
---            '?rating :rating/book ?calvin' \
---            '?rating :rating/score 1' \
---            '?rating :rating/user ?u' \
---            '?more-great-takes :rating/user ?u' \
---            '?more-great-takes :rating/book ?b' \
---            '?more-great-takes :rating/score 5' \
---     --show '?b :book/title :book/avg-rating' \
---     --asc  '?b :book/avg-rating' --db /tmp/owoof-three.sqlite
-- The following index is very slow to populate :(
--- create index "triples-v" on "triples" (v);

-- When creating a triple, increment the value's soup.rc.
create trigger "triples/soup-inc-rc" after insert
            on "triples"
begin update "soup" set rc = rc + 1 where rowid = new.v;
end;

-- When removing a triple, decrement the value's soup.rc.
create trigger "triples/soup-dec-rc" after delete
            on "triples"
begin update "soup" set rc = rc - 1 where rowid = old.v;
end;

--------------------------------------------------------------------------------
-- "entities" is a materialized view maintained by triggers.

create table "entities"
    ( rowid  integer primary key  references "soup" (rowid) );

create trigger "soup/replicate-entities" after insert
            on "soup" when new.t = 1
begin insert into "entities" (rowid) values (new.rowid);
end;

create trigger "soup/unreplicate-entities" after delete
            on "soup" when old.t = 1
begin delete from "entities" where rowid = old.rowid;
end;

-- When an entity id is encoded, a row in soup with t = 1 is inserted.  This row must
-- have a corresponding row in "triples"  (soup.rowid :db/id soup.rowid)  for its entire
-- lifetime and a row in "entities" so that the triple is valid.
create trigger "soup/assert-dbid-triples" after insert
            on "soup" when new.t = 1 -- src/driver.rs ENTITY_ID_TAG
-- a = 1, the soup rowid for :db/id
begin insert into "triples" (e, a, v) values (new.rowid, 1, new.rowid);
end;

-- If the triple is deleted, the "entities" and "soup" rows must also be deleted,
-- meaning they can't be referenced anywhere else. So you can't delete  (a :db/id a)
-- while  (b :buddy/friend a)  exists or while  (a :pet/name "Spot")  exists.
create trigger "triples/retract-dbid-soup" after delete
            on "triples" when old.a = 1 -- the soup rowid for :db/id
                          and old.e = old.v
begin delete from "soup" where rowid = old.e;
end;


--------------------------------------------------------------------------------
-- "attributes" is a materialized view maintained by triggers.

create table "attributes"
    -- rowid points to the attribute's uuid value in "soup" ... not the identifier
    ( rowid  integer primary key  references "entities" (rowid)
    , ident  integer not null     references "soup" (rowid) );

create unique index "attribute-ident-unique" on "attributes" (ident);

create trigger "triples/no-updates" before update
            on "triples"
begin select raise (abort, 'not yet implemented (also confusing)');
end;

-- tiggers to maintain the attributes materialized view

create trigger "triples/replicate-attributes" after insert
            on "triples" when new.a = 3  -- :db/attributes's :db/id
begin insert into "attributes" (rowid, ident) values (new.e, new.v);
end;

create trigger "triples/unreplicate-attributes" after delete
            on "triples" when old.a = 3  -- :db/attributes's :db/id
begin delete from "attributes" where rowid = old.e;
end;


-- Initial data; the entity and attribute identifier for :db/id and :db/attribute.

-- TODO is randomblob(16) a valid v4 uuid?
insert into "soup" (rowid, t, v)
     values (1, 1, randomblob(16))  -- :db/id's :db/id
          , (2, 2, "db/id")
          , (3, 1, randomblob(16))  -- :db/attribute's :db/id
          , (4, 2, "db/attribute");

-- Also, two other triples should already exist (v :db/id v)
-- for both :db/id and :db/attribute entities.
insert into "triples" (e, a, v)
     values (1, 3, 2)  -- :db/id        :db/attribute :db/id
          , (3, 3, 4)  -- :db/attribute :db/attribute :db/attribute
    ;


-- commit;
