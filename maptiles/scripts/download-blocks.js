const { default: Queue } = require('p-queue');
const wget = require('wget-improved');

const queue = new Queue({ concurrency: 2 });

function downloadBlocksForFips(fips) {
  const url = `https://www2.census.gov/geo/tiger/TIGER2019/TABBLOCK/tl_2019_${fips}_tabblock10.zip`;
  const dest = `./geographies/blocks/${fips}_blocks.zip`;

  return new Promise((resolve, reject) => {
    const download = wget.download(url, dest);

    download.on('error', function(err) {
      console.error(`Error downloading ${fips}:`, err);
      console.error(`\t${url}`);
      reject(err);
    });

    download.on('end', function() {
      console.log(`Downloaded ${fips}`);
      resolve();
    });
  });
}

function addToQueue(fips) {
  const nonFips = ['03', '07', '14', '43', '52'];
  return async () => {
    if (nonFips.includes(fips)) {
      console.log(`Skipping ${fips} because it isn't a US state`);
      return;
    }

    try {
      console.log(`Downloading ${fips}...`);
      await downloadBlocksForFips(fips);
    } catch {
      console.log(`Error with ${fips}, adding back to queue`);
      addToQueue(fips);
    }
  };
}

for (let i = 1; i < 57; i++) {
  const fips = i < 10 ? `0${i}` : `${i}`;
  queue.add(addToQueue(fips));
}

queue.onIdle().then(() => {
  console.log(`All done downloading shapefiles`);
  return;
});
