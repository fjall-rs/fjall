import { spawn } from "node:child_process";
import { readdir } from "node:fs/promises";
import { resolve } from "node:path";

const examplesFolder = "examples";

for (const exampleName of await readdir(examplesFolder)) {
  const folder = resolve(examplesFolder, exampleName);

  {
    const proc = spawn("cargo build", {
      cwd: folder,
      shell: true,
    });

    proc.stdout.on("data", buf => console.log(String(buf)));
    proc.stderr.on("data", buf => console.error(String(buf)));

    await new Promise((resolve, _) => {
      proc.on("exit", () => {
        if (proc.exitCode > 0) {
          console.error(`${folder} FAILED`);
          process.exit(1);
        }
        else {
          resolve();
        }
      })
    });
  }

  {
    const proc = spawn("cargo test", {
      cwd: folder,
      shell: true,
    });

    proc.stdout.on("data", buf => console.log(String(buf)));
    proc.stderr.on("data", buf => console.error(String(buf)));

    await new Promise((resolve, _) => {
      proc.on("exit", () => {
        if (proc.exitCode > 0) {
          console.error(`${folder} FAILED`);
          process.exit(1);
        }
        else {
          resolve();
        }
      })
    });
  }

  console.error(`${folder} OK`);
}
