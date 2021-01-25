const fs = require('fs');

const { parse, transform } = require('csv');
const sqlite = require('sqlite3');
const { open } = require('sqlite');

const args = process.argv;
const inputFileArg = args[2];
const dbFileName = `./fcc-477.sqlite`;

(async () => {
  const db = await open({
    filename: dbFileName,
    driver: sqlite.Database,
  });
  await db.run(
    `CREATE TABLE rows(block_fips TEXT,tract_fips TEXT,county_fips TEXT,max_ad_down,max_ad_up,provider_id);`,
  );

  let rowCount = 0;

  async function processRow(row, cb) {
    const consumer = row['Consumer'];
    const providerId = row['Provider ID'];
    const blockCode = row['Census Block FIPS Code'];
    const maxAdDown = row['Max Advertised Downstream Speed (mbps)'];
    const maxAdUp = row['Max Advertised Upstream Speed (mbps)'];
    if (consumer !== '1') return cb(null);
    const tractFips = blockCode.slice(0, 11);
    const countyFips = blockCode.slice(0, 5);

    try {
      const sql = `INSERT INTO rows (block_fips,tract_fips,county_fips,max_ad_down,max_ad_up,provider_id) VALUES ("${blockCode}","${tractFips}","${countyFips}",${+maxAdDown},${+maxAdUp},${providerId})`;
      await db.run(sql);
      rowCount += 1;
      if (rowCount % 100000 === 0) {
        console.log(`Stored ${rowCount} rows in the database`);
      }
      cb(null, blockCode);
    } catch (err) {
      console.log({ row });
      cb(err);
    }
  }

  console.log(`Loading ${inputFileArg} into ${dbFileName}`);

  const input = fs.createReadStream(inputFileArg);
  const parser = parse({ columns: true });
  const transformer = transform(processRow);

  transformer.on('end', async () => {
    console.log(`Added ${rowCount} rows, creating indexes now`);
    await db.run(`CREATE INDEX block_fips_index ON rows (block_fips);`);
    await db.run(`CREATE INDEX county_fips_index ON rows (county_fips);`);
    await db.run(`CREATE INDEX tract_fips_index ON rows (tract_fips);`);
    console.log(`Generated ${dbFileName}`);
  });

  input.pipe(parser).pipe(transformer);
  transformer.resume();
})();
