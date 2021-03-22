const { exec: execWithCallback } = require('child_process');

const { default: Queue } = require('p-queue');

function exec(args) {
  return new Promise((resolve, reject) => {
    execWithCallback(args, { maxBuffer: 2000 * 1024 }, (err, data) => {
      if (err) return reject(err);
      resolve(data);
    });
  });
}

const args = process.argv;
const geographicLevel = args[2] || 'counties'; // can also be "tracts"
const rootGsUrl = `gs://statistics-mlab-sandbox/v0/NA/US/${geographicLevel}`;

let fileCount = 0;

async function download(fips, i) {
  const cmd = `gsutil cp ${rootGsUrl}/${fips}/2019/histogram_daily_stats.json mlab/${geographicLevel}/${fips}.json`;
  try {
    console.log(`Downloading JSON file (FIPS: ${fips}, ${i} / ${fileCount})`);
    await exec(cmd);
  } catch (e) {
    console.error(`Error with ${fips}\n\t`, e);
  }
}

async function main() {
  console.log(`Finding MLab data files to download for ${geographicLevel}`);
  const filesList = await exec(`gsutil ls ${rootGsUrl}/`);
  const files = filesList.split('\n');
  fileCount = files.length;

  console.log(`Found ${fileCount} files to download, starting now`);

  const queue = new Queue({ concurrency: 16 });
  const fipsLength = geographicLevel === 'counties' ? '5' : '11';
  const r = new RegExp(`${geographicLevel}\\/(\\d{${fipsLength}})\\/$`);

  files.forEach((file, fileIndex) => {
    const match = file.match(r);
    if (!match) return;

    const fips = match[1];
    queue.add(async () => {
      await download(fips, fileIndex);
    });
  });

  await queue.onIdle();

  console.log(`Downloaded ${files.length} JSON files`);
}

main();
