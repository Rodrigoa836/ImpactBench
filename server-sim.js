// One simulation per visitor, on its own thread. It runs the same physics code the page uses.
const { parentPort } = require('worker_threads');
// Node timers have no 4 ms clamp, and with a message channel the loop stalled in a worker thread, so use timers
globalThis.MessageChannel = undefined;
const fs = require('fs'), path = require('path');
const js = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8').split('<script>')[1].split('</script>')[0];
const cut = js.search(/\(function \(\) \{\s*'use strict';\s*var WW/);
if (cut < 0) throw new Error('Could not find the page code in index.html');
const { simCore } = new Function(js.slice(0, cut) + '\nreturn { makeMats: makeMats, simCore: simCore };')();
const self = { postMessage: (m, tr) => parentPort.postMessage(m, tr || []), onmessage: null };
simCore(self);
parentPort.on('message', (m) => { if (self.onmessage) self.onmessage({ data: m }); });
