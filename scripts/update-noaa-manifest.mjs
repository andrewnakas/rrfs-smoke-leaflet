import { spawn } from 'node:child_process';

const child = spawn('python3', ['./scripts/render-rrfs-smoke.py'], {
  stdio: 'inherit',
  shell: false,
});

child.on('exit', (code) => {
  process.exit(code ?? 1);
});
