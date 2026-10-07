/** `pnpm verify`: the one definition of done. Steps run in order; the first red stops. */
import { spawnSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const steps: Array<[name: string, cwd: string, cmd: Array<string>]> = [
  ["typecheck", ROOT, ["tsc", "-p", "tsconfig.json"]],
  ["lint", ROOT, ["vp", "lint", "--deny-warnings"]],
  ["fmt", ROOT, ["vp", "fmt", "--check"]],
  ["test", ROOT, ["vp", "run", "-r", "test"]],
];

for (const [name, cwd, [bin, ...args]] of steps) {
  const started = Date.now();
  console.log(`\n== verify: ${name}  (${[bin, ...args].join(" ")})`);
  const r = spawnSync(bin!, args, { cwd, stdio: "inherit", shell: process.platform === "win32" });
  const seconds = ((Date.now() - started) / 1000).toFixed(1);
  if (r.status !== 0) {
    console.error(`\n== verify: ${name} RED after ${seconds}s (exit ${r.status})`);
    process.exit(r.status ?? 1);
  }
  console.log(`== verify: ${name} green in ${seconds}s`);
}
console.log("\n== verify: green");
