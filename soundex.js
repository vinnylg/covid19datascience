const fs = require('fs');
const readFileSync = fs.readFileSync
const writeFileSync = fs.writeFileSync

const metaphone = require('metaphone-ptbr');
// const cliProgress = require('cli-progress');

// create a new progress bar instance and use shades_classic theme
// const bar1 = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);
// start the progress bar with a total value of 200 and start value of 0

let obj = JSON.parse(readFileSync('pacientes/pacientes.json', 'utf8'))
let pacientes = obj['pacientes']

// bar1.start(pacientes.length, 0);

for(let idx = 0; idx < pacientes.length; idx++){
    // bar1.update(idx)
    pacientes[idx] = metaphone(pacientes[idx])
}

// bar1.stop()

obj = JSON.stringify(pacientes);

writeFileSync('pacientes/pacientes_soundex.json', obj);