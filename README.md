Converts `.stc` tables into `.csv` files and splits `catchdata.dat` into `.json` files.

# Data versioning
During login sequence, game client queries `Index/version` endpoint to check if client is up-to-date.

If `data_version` in response differs from the `version` in client preferences, update is initiated.

The following function is used to get the URL:

```python
from hashlib import md5

def get_data_file_full_url(version: str) -> str:
    return f'http://dkn3dfwjnmzcj.cloudfront.net/data/stc_{version}{md5(version.encode()).hexdigest()}.zip'
```

After completing the download, the archive is placed at `<internal storage>/Android/data/com.sunborn.girlsfrontline.en/files/stc_data.dat` and then contents extracted in `<internal storage>/Android/data/com.sunborn.girlsfrontline.en/files/stc` directory.

*※ Game client preferences location is `/data/data/com.sunborn.girlsfrontline.en/shared_prefs/com.sunborn.girlsfrontline.en.v2.playerprefs.xml`*

# `.stc`

## Format overview
```
    +---+---+---+---+---+---+
    |  id   |  lbs  |  rows |
    +---+---+---+---+---+---+
    +---+===================+
    | c |    column types   |
    +---+===================+
    +=======================+
    |       jump table      |
    +=======================+
    +=======================+
    |          data         |
    +=======================+
```
where:
- `id` is a `u16` integer representing ID of this table, usually matches the file name, e.g. `5000.stc` will have ID of 5000
- `lbs`, is a `u16` length of the last 65536 byte block, not counting `id` and itself
- `rows` is a `u16` number of rows in this table
- `c` is a `u8` number of columns in a row
- `column types` is a sequence of `u8` integers with `c` length, each value represents column type in a row as follows:
    - 1 => `i8`
    - 2 => `u8`
    - 3 => `i16`
    - 4 => `u16`
    - 5 => `i32`
    - 6 => `u32`
    - 7 => `i64`
    - 8 => `u64`
    - 9 => `f32`
    - 10 => `f64`
    - 11 => `string` with the structure as follows:
        ```
        +---+---+---+======+
        | a |  len  |  str |
        +---+---+---+======|
        ```
        where `a` is `is_ascii` flag, `str` is ASCII or UTF-8 encoded
- `jump table` is a sequence of two `i32` and `u32` integers (`row_id` and absolute `offset`) of every 100th row starting with first row (with 0-based indexing: 0th, 100th, 200th, etc)
- `data` is a sequence of `rows`, each row have `c` columns

*※ Little-endian ordering is used*

## Table definitions
STC tables themselves don't define their name or column names, a way to acquire them is to dump headers using [il2cpp dumper](https://github.com/Perfare/Il2CppDumper).

il2cpp metadata is encrypted, method unknown, use [GameGuardian](https://gameguardian.net/download) to dump memory and acquire decrypted file.

*※ Do this on an offline virtual device in case some security measure gets tripped*

*※ Enable `Hide GameGuardian from the game` option before dumping memory*

After dumping headers:
- look for `CmdDef` enum to find table names
- look for classes starting with `Stc` in the name to find column names


# `catchdata.dat`

Essentially is a `.jsonl` file that is gzip compressed and XOR encrypted.

The decryption function is as follows:

```python
key = b'c88d016d261eb80ce4d6e41a510d4048'

def xor(buffer: bytearray, key: bytes):
    for i in range(len(buffer)):
        buffer[i] ^= key[i % len(key)]
```