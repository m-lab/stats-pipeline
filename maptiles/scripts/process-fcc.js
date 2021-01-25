const fs = require('fs');

const sqlite = require('sqlite3');
const { open } = require('sqlite');
const { default: Queue } = require('p-queue');

const args = process.argv;
const geographyType = args[2] || 'county'; // can also be "tract"

let count = 0;
const geographyColumn = `${geographyType}_fips`;
let fipsLength = geographyType === 'county' ? 5 : 11;
let isFirst = true;
const outputFile = `fcc-${geographyType}.json`;
const queue = new Queue({ concurrency: 50 });
const writeStream = fs.createWriteStream(outputFile);

function mean(arr) {
  if (arr.length === 0) return 0;

  const sum = arr.reduce((accum, next) => {
    return accum + next;
  }, 0);
  return sum / arr.length;
}

async function processId(id, db) {
  count += 1;
  const query = `SELECT * FROM rows WHERE ${geographyColumn} = "${id}";`;

  const downs = [];
  const ups = [];
  const providers = new Set();
  const rows = await db.all(query);
  console.log(`${rows.length} rows to analyze for ${id} (#${count})`);

  rows.forEach(r => {
    const { max_ad_down, max_ad_up, provider_id } = r;

    downs.push(+max_ad_down);
    ups.push(+max_ad_up);
    providers.add(provider_id);
  });

  const delim = isFirst ? '' : ',';
  if (isFirst) isFirst = false;

  const json = JSON.stringify({
    geo_id: `${id}`.padStart(fipsLength, '0'), // block code for blocks and fips for counties
    provider_count: providers.size,
    mean_max_ad_down: mean(downs),
    mean_max_ad_up: mean(ups),
    source_rows: rows.length,
  });

  writeStream.write(`${delim}\n${json}`);
  return;
}

(async () => {
  const db = await open({
    filename: `./fcc-477.sqlite`,
    driver: sqlite.Database,
    mode: sqlite.OPEN_READONLY,
  });

  sqlite.verbose();

  const ids = await db.all(`SELECT DISTINCT ${geographyColumn} FROM rows;`);

  writeStream.write('[');
  console.log(`Found ${ids.length} unique values for ${geographyColumn}`);

  await db.each(`SELECT DISTINCT ${geographyColumn} FROM rows;`, (err, row) => {
    if (err) {
      console.error(err);
      return;
    }

    const id = row[geographyColumn];
    queue.add(() => processId(id, db));
  });

  await queue.onIdle();
  writeStream.write(']');
})();
