# RethinkDB Python driver
[![PyPI version](https://badge.fury.io/py/rethinkdb.svg)](https://badge.fury.io/py/rethinkdb) [![Build Status](https://travis-ci.org/rethinkdb/rethinkdb-python.svg?branch=master)](https://travis-ci.org/rethinkdb/rethinkdb-python) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/2b5231a6f90a4a1ba2fc795f8466bbe4)](https://www.codacy.com/app/rethinkdb/rethinkdb-python?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=rethinkdb/rethinkdb-python&amp;utm_campaign=Badge_Grade) [![Codacy Badge](https://api.codacy.com/project/badge/Coverage/2b5231a6f90a4a1ba2fc795f8466bbe4)](https://www.codacy.com/app/rethinkdb/rethinkdb-python?utm_source=github.com&utm_medium=referral&utm_content=rethinkdb/rethinkdb-python&utm_campaign=Badge_Coverage)

## Overview

### What is RethinkDB?
RethinkDB is the first open-source scalable database built for realtime applications. It exposes a new database access model -- instead of polling for changes, the developer can tell the database to continuously push updated query results to applications in realtime. RethinkDB allows developers to build scalable realtime apps in a fraction of the time with less effort.

## Installation
```bash
$ pip install rethinkdb
```
*Note: this package is the extracted driver of RethinkDB's original python driver.*

## Quickstart
The main difference with the previous driver (except the name of the package) is we are **not** importing RethinkDB as `r`. If you would like to use `RethinkDB`'s python driver as a drop in replacement, you should do the following:

```python
from rethinkdb import r

connection = r.connect(db='test')
```

## Blocking and Non-blocking I/O
This driver supports blocking I/O (i.e. standard Python sockets) as well as
non-blocking I/O through multiple async frameworks:

* [Asyncio](https://docs.python.org/3/library/asyncio.html)
* [Gevent](http://www.gevent.org/)
* [Tornado](https://www.tornadoweb.org/en/stable/)
* [Trio](https://trio.readthedocs.io/en/latest/)
* [Twisted](https://twistedmatrix.com/trac/)

The following examples demonstrate how to use the driver in each mode.

### Default mode (blocking I/O)
The driver's default mode of operation is to use blocking I/O, i.e. standard Python
sockets. This example shows how to create a table, populate with data, and get every
document.

```python
from rethinkdb import r

connection = r.connect(db='test')

r.table_create('marvel').run(connection)

marvel_heroes = r.table('marvel')
marvel_heroes.insert({
    'id': 1,
    'name': 'Iron Man',
    'first_appearance': 'Tales of Suspense #39'
}).run(connection)

for hero in marvel_heroes.run(connection):
    print(hero['name'])
```

### Asyncio mode
Asyncio mode requires Python ≥ 3.9 (the package floor).

```python
import asyncio
from rethinkdb import r

async def main():
    async with await r.connect(db='test') as connection:
        await r.table_create('marvel').run(connection)

        marvel_heroes = r.table('marvel')
        await marvel_heroes.insert({
            'id': 1,
            'name': 'Iron Man',
            'first_appearance': 'Tales of Suspense #39'
        }).run(connection)

        cursor = await marvel_heroes.run(connection)
        async for hero in cursor:
            print(hero['name'])
    # The `with` block performs `await connection.close(noreply_wait=False)`.

r.set_loop_type('asyncio')

asyncio.run(main())
```

### Gevent mode

```python
import gevent
from rethinkdb import r

def main():
    r.set_loop_type('gevent')
    connection = r.connect(db='test')

    r.table_create('marvel').run(connection)

    marvel_heroes = r.table('marvel')
    marvel_heroes.insert({
        'id': 1,
        'name': 'Iron Man',
        'first_appearance': 'Tales of Suspense #39'
    }).run(connection)

    for hero in marvel_heroes.run(connection):
        print(hero['name'])

gevent.joinall([gevent.spawn(main)])
```

### Tornado mode
Tornado mode is compatible with Tornado < 5.0.0. Tornado 5 is not supported.

```python
from rethinkdb import r
from tornado import gen
from tornado.ioloop import IOLoop

@gen.coroutine
def main():
    r.set_loop_type('tornado')
    connection = yield r.connect(db='test')

    yield r.table_create('marvel').run(connection)

    marvel_heroes = r.table('marvel')
    yield marvel_heroes.insert({
        'id': 1,
        'name': 'Iron Man',
        'first_appearance': 'Tales of Suspense #39'
    }).run(connection)

    cursor = yield marvel_heroes.run(connection)
    while (yield cursor.fetch_next()):
        hero = yield cursor.next()
        print(hero['name'])

IOLoop.current().run_sync(main)
```

### Trio mode

```python
from rethinkdb import r
import trio

async def main():
    r.set_loop_type('trio')
    async with trio.open_nursery() as nursery:
        async with r.open(db='test', nursery=nursery) as conn:
            await r.table_create('marvel').run(conn)
            marvel_heroes = r.table('marvel')
            await marvel_heroes.insert({
                'id': 1,
                'name': 'Iron Man',
                'first_appearance': 'Tales of Suspense #39'
            }).run(conn)

            cursor = await marvel_heroes.run(conn)
            async with cursor:
                async for hero in cursor:
                    print(hero['name'])

trio.run(main)
```

The Trio mode also supports a database connection pool. You can modify the example above
as follows:

```python
db_pool = r.ConnectionPool(db='test', nursery=nursery)
async with db_pool.connection() as conn:
    ...
await db_pool.close()
```

### Twisted mode

```python
from rethinkdb import r
from twisted.internet import reactor, defer

@defer.inlineCallbacks
def main():
    r.set_loop_type('twisted')
    connection = yield r.connect(db='test')

    yield r.table_create('marvel').run(connection)

    marvel_heroes = r.table('marvel')
    yield marvel_heroes.insert({
        'id': 1,
        'name': 'Iron Man',
        'first_appearance': 'Tales of Suspense #39'
    }).run(connection)

    cursor = yield marvel_heroes.run(connection)
    while (yield cursor.fetch_next()):
        hero = yield cursor.next()
        print(hero['name'])

main().addCallback(lambda d: print("stopping") or reactor.stop())
reactor.run()
```

## Misc
To help the migration from rethinkdb<2.4 we introduced a shortcut which can easily replace the old `import rethinkdb as r` import with `from rethinkdb import r`.

## Run tests
In the `Makefile` you can find three different test commands: `test-unit`, `test-integration` and `test-remote`. As RethinkDB has dropped the support of Windows, we would like to ensure that those of us who are using Windows for development can still contribute. Because of this, we support running integration tests against Digital Ocean Droplets as well.

Before you run any test, make sure that you install the package with the
test (and optionally dev) extras and prepare the protobuf stubs:
```bash
$ pip install -e ".[test,dev]"
$ make prepare
```

To pull in a specific async backend's dependencies, use the matching
extra: `pip install "rethinkdb[tornado]"`, `[trio]`, `[gevent]`, or
`[twisted]`. The default install ships with the asyncio and sync
backends only.

### Running unit tests
```bash
$ make test-unit
```

### Running integration tests
*To run integration tests locally, make sure you intstalled RethinkDB*
```bash
$ make test-integration
```

### Running remote integration tests
*To run the remote tests, you need to have a Digital Ocean account and an API key.*

Remote test will create a new temporary SSH key and a Droplet for you until the tests are finished.

**Available environment variables**

| Variable name | Default value |
|---------------|---------------|
| DO_TOKEN      | N/A           |
| DO_SIZE       | 512MB         |
| DO_REGION     | sfo2          |

```bash
$ pip install paramiko python-digitalocean
$ export DO_TOKEN=<YOUR_TOKEN>
$ make test-remote
```

## Contributing
Hurray! You reached this section which means, that you would like to contribute. Please read our contributing guide lines and feel free to open a pull request.
