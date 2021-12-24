owoof
=====

[<img alt="github" src="https://img.shields.io/badge/github-sqwishy/owoof-076678?style=for-the-badge&labelColor=282828&logo=github" height="22">](https://github.com/sqwishy/owoof)
[<img alt="crates.io" src="https://img.shields.io/crates/v/owoof.svg?style=for-the-badge&color=282828&labelColor=282828&logo=rust" height="22">](https://crates.io/crates/owoof)
[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-owoof-427b58?style=for-the-badge&labelColor=282828" height="22">](https://docs.rs/owoof)

A glorified query-builder inspired by [Datomic](https://docs.datomic.com/cloud/index.html)
that uses a datalog-like format for querying and modifying information around a SQLite
database.

This is a pet project and probably shouldn't be used for anything serious.

This is implemented as a rust library. It is documented, you can read the source or
maybe find the [documentation published on docs.rs](https://docs.rs/owoof/*/owoof/).

There are two rust executable targets.  One provides a command-line-interface (as shown
below) and another can be used for importing data from a csv file.

## CLI

Compile this with `cargo build` using `--features cli --bin cli`.

The CLI can be used to initialize new database files, assert/create, retract/remove, or
query information.

Here are some examples:

```json
$ echo '[{":db/attribute": ":pet/name"},
         {":pet/name": "Garfield"},
         {":pet/name": "Odie"},
         {":pet/name": "Spot"},
         {":db/attribute": ":person/name"},
         {":db/attribute": ":person/starship"},
         {":person/name": "Jon Arbuckle"},
         {":person/name": "Lieutenant Commander Data",
          ":person/starship": "USS Enterprise (NCC-1701-D)"}]' \
      | owoof assert
[
  "#45e9d8e9-51ea-47e6-8172-fc8179f8fbb7",
  "#4aa95e29-8d45-470b-98a7-ee39aae1b9c9",
  "#2450b9e6-71a4-4311-b93e-3920eebb2c06",
  "#c544251c-a279-4809-b9b6-7d3cd68d2f2c",
  "#19a4cba1-6fc7-4904-ad36-e8502445412f",
  "#f1bf032d-b036-4633-b6f1-78664e44603c",
  "#e7ecd66e-222f-44bc-9932-c778aa26d6ea",
  "#af32cfdb-b0f1-4bbc-830f-1eb83e4380a3"
]

$ echo '[{":db/attribute": ":pet/owner"},
         {":db/id": "#4aa95e29-8d45-470b-98a7-ee39aae1b9c9",
          ":pet/owner": "#e7ecd66e-222f-44bc-9932-c778aa26d6ea"},
         {":db/id": "#2450b9e6-71a4-4311-b93e-3920eebb2c06",
          ":pet/owner": "#e7ecd66e-222f-44bc-9932-c778aa26d6ea"},
         {":db/id": "#c544251c-a279-4809-b9b6-7d3cd68d2f2c",
          ":pet/owner": "#af32cfdb-b0f1-4bbc-830f-1eb83e4380a3"}]' \
      | owoof assert
[
  "#ffc46ae2-1bde-4c08-bfea-09db8241aa2b",
  "#4aa95e29-8d45-470b-98a7-ee39aae1b9c9",
  "#2450b9e6-71a4-4311-b93e-3920eebb2c06",
  "#c544251c-a279-4809-b9b6-7d3cd68d2f2c"
]

$ owoof  '?pet :pet/owner ?owner' \
  --show '?pet :pet/name' \
  --show '?owner :person/name'
[
  [
    { ":pet/name": "Garfield" },
    { ":person/name": "Jon Arbuckle" }
  ],
  [
    { ":pet/name": "Odie" },
    { ":person/name": "Jon Arbuckle" }
  ],
  [
    { ":pet/name": "Spot" },
    { ":person/name": "Lieutenant Commander Data" }
  ]
]

$ owoof '?person :person/starship "USS Enterprise (NCC-1701-D)"' \
        '?pet :pet/owner ?person' \
        '?pet :pet/name ?n'
[
  "Spot"
]

```

Imported from the [goodbooks-10k](https://github.com/zygmuntz/goodbooks-10k) dataset.

```json
$ owoof '?r :rating/score 1' \
        '?r :rating/book ?b' \
        '?b :book/authors "Dan Brown"' \
 --show '?r :rating/user' \
 --show '?b :book/title' \
 --limit 5
[
  [
    { ":rating/user": 9 },
    { ":book/title": "Angels & Demons  (Robert Langdon, #1)" }
  ],
  [
    { ":rating/user": 58 },
    { ":book/title": "The Da Vinci Code (Robert Langdon, #2)" }
  ],
  [
    { ":rating/user": 65 },
    { ":book/title": "The Da Vinci Code (Robert Langdon, #2)" }
  ],
  [
    { ":rating/user": 80 },
    { ":book/title": "The Da Vinci Code (Robert Langdon, #2)" }
  ],
  [
    { ":rating/user": 89 },
    { ":book/title": "The Da Vinci Code (Robert Langdon, #2)" }
  ]
]
```

## TODO/Caveats

- Testing is not extensive at this point.

  The schema _should_ be enforced, so no deleting attributes that are in use, but I
  haven't done the work to verify this so there might be some surprises.

- Performance is not super reliable.

  Version 0.2 adds partial indexes over specific attributes and has helped a lot with
  search performance.  However, there is no index on values.  Some queries are
  impacted by this more than others, so performance is not reliable.

  The difficulty currently with a values index is that SQLite's query planner will
  prefer it in cases where it shouldn't.  It isn't a good index and should be a
  last-resort -- it's also huge.

- This is not feature-rich yet, constraints ensure equality and no support for
  constraints over ranges or involving logical operations exist yet and honestly I
  haven't tested how well it will perform with the schema changes made in 0.2.

- owoof-csv needs a way to remap ids, so instead `:rating/book` joining with `:book/id`
  it joins to `:db/id`.

## Internal TODOs

- Actually address the csv import id remap above.

- Create DontWoof off the Connection.

- The Select borrowing Network is a bit weird.  I tried to split it off but it was still
  weird.  Not sure what to do about that.  One consideration is that pushing a Select on
  to a Query only borrows from the network.  Maybe this could be relaxed?

- Test reference counting?  Add a clean-up that removes soups with zero rc and runs
  pragma optimize.

- Maybe add some sort of update thing to shorthand retract & assert?

## See Also

My blog post associated with version 0.1 this software: https://froghat.ca/blag/dont-woof

#### License

<sup>This is licensed under [Apache License, Version 2.0](LICENSE).</sup>
