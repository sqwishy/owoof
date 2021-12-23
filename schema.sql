-- pragma foreign_keys=on;
-- begin;
-- FYI: "At this time SQLite supports only FOR EACH ROW triggers"


-- ...

create table "soup"
    ( rowid  integer primary key
    , t      integer not null
    , v      blob    not null
    , rc     integer not null default 0
    -- , check (t != 1 OR length(v) == 16)
    );
-- create unique index "soup-tv-cover" on "soup" (t, v);
-- I guess t values are decided at compile time? So we can do this fairly reliably?
-- Apparently sqlite doesn't consider these t covering indexes? thonkingface
-- This seems to improve query time a bit but insert performance is much slower so idk.
create unique index "soup-tv-0" on "soup" (v) where t = 0;
create unique index "soup-tv-1" on "soup" (v) where t = 1;
create unique index "soup-tv-2" on "soup" (v) where t = 2;

create trigger "soup/no-updates" before update
            on "soup" when new.t != old.t or new.v != old.v
begin select raise (abort, 'not yet implemented (also confusing)');
end;


-- a materialized view, maintained by a soup trigger, do not touch

create table "entities"
    ( rowid  integer primary key  references "soup" (rowid) );

-- triggers to maintain the `entities` materialized view

create trigger "soup/replicate-entities" after insert
            on "soup" when new.t = 1
begin insert into "entities" (rowid) values (new.rowid);
end;

create trigger "soup/unreplicate-entities" after delete
            on "soup" when old.t = 1
begin delete from "entities" where rowid = old.rowid;
end;

-- triggers to replicate the `:db/id` triples

create trigger "soup/assert-dbid-triples" after insert
            on "soup" when new.t = 1
begin insert into "triples" (e, a, v) values (new.rowid, 1, new.rowid);
end;

create trigger "soup/retract-dbid-triples" after delete
            on "soup" when old.t = 1
begin delete from "triples" where e = old.rowid and a = 1 and v = old.rowid;
end;


-- a materialized view, maintained by a trigger, do not touch

create table "attributes"
    -- rowid points to the attribute's uuid value in "soup" ... not the identifier
    ( rowid  integer primary key  references "entities" (rowid)
    , ident  integer not null     references "soup" (rowid) );

create unique index "attribute-ident-unique" on "attributes" (ident);

-- ...

create table "triples"
    ( e  integer not null  references "entities" (rowid)
    , a  integer not null  references "attributes" (rowid)
    , v  integer not null  references "soup" (rowid)
    , primary key (e, a, v)
    ) without rowid;  -- <_<

-- This is very slow to populate :(
-- TODO this kills the following query...
--     owoof '?calvin :book/title "The Complete Calvin and Hobbes"' \
--            '?rating :rating/book ?calvin' \
--            '?rating :rating/score 1' \
--            '?rating :rating/user ?u' \
--            '?more-great-takes :rating/user ?u' \
--            '?more-great-takes :rating/book ?b' \
--            '?more-great-takes :rating/score 5' \
--     --show '?b :book/title :book/avg-rating' \
--     --asc  '?b :book/avg-rating' --db /tmp/owoof-three.sqlitep
-- create index "triples-v" on "triples" (v);

-- created programatically by the owoof library
-- create index "triples-ave-N"  on "triples" (v, e) where a =  N;


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

-- tiggers to reference count soups

create trigger "triples/soup-inc-rc" after insert
            on "triples"
begin update "soup" set rc = rc + 1 where rowid = new.v;
end;

create trigger "triples/soup-dec-rc" after delete
            on "triples"
begin update "soup" set rc = rc - 1 where rowid = old.v;
end;


-- ...

-- TODO is randomblob(16) a valid v4 uuid?
insert into "soup" (rowid, t, v)
     values (1, 1, randomblob(16))  -- :db/id's :db/id
          , (2, 2, "db/id")
          , (3, 1, randomblob(16))  -- :db/attribute's :db/id
          , (4, 2, "db/attribute");

insert into "triples" (e, a, v)
     values (1, 3, 2)  -- :db/id        :db/attribute :db/id
          , (3, 3, 4)  -- :db/attribute :db/attribute :db/attribute
    ;

-- commit;
