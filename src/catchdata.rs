use flate2::bufread::GzDecoder;

use std::fs;
use std::io::{self, BufRead, Cursor, Read};
use std::path::Path;

const KEY: &[u8] = b"c88d016d261eb80ce4d6e41a510d4048";

pub(crate) fn parse<P>(path: P) -> io::Result<()>
where
    P: AsRef<Path>,
{
    let path = path.as_ref();
    let mut data = fs::read(path)?;

    // decrypt
    for i in 0..data.len() {
        data[i] ^= KEY[i % KEY.len()]
    }

    // decompress
    let data = {
        let mut gz = GzDecoder::new(&data[..]);
        let mut de = Vec::new();
        gz.read_to_end(&mut de)?;
        Cursor::new(de)
    };

    // split
    for line in data.lines() {
        let line = line?;
        // starting from second line, there's 6 spaces padding at the start
        let line = line.trim();

        if line.is_empty() {
            continue;
        }

        for (key, entry) in json::parse(line)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
            .entries()
        {
            let name = format!("{}.json", key);
            let data = entry.pretty(2);

            fs::write(path.with_file_name(name), data)?;
        }
    }

    Ok(())
}
