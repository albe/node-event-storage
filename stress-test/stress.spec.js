/**
 * Crash-safety stress test – mocha spec.
 *
 * Runs the bash orchestration script (run.sh) as a child process.  The script:
 *  1. Starts the writer process (writer.js) in the background.
 *  2. Lets it write events for a configurable number of seconds.
 *  3. Kills the writer with SIGKILL (simulates a hard crash).
 *  4. Runs the recovery script (recovery.js) which opens the store with
 *     LOCK_RECLAIM, verifies readability and writability, and checks that
 *     data loss is within the expected bounds.
 *
 * Set the WRITE_DURATION environment variable (default: 5 seconds) to control
 * how long the writer runs before being killed.
 */

import { spawn } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = fileURLToPath(new URL('.', import.meta.url));

describe('Crash-safety stress test', function () {
    this.timeout(30_000);

    it('recovers from a hard crash with bounded data loss', function (done) {
        const runSh = path.join(__dirname, 'run.sh');
        const child = spawn('bash', [runSh], { stdio: 'inherit' });

        child.on('close', (code) => {
            if (code !== 0) {
                return done(new Error(`Stress test script exited with code ${code}.`));
            }
            done();
        });

        child.on('error', done);
    });
});
