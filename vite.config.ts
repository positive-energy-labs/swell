import { defineConfig } from "vite-plus";

const ignore = ["**/dist/**", "**/node_modules/**", "**/.artifacts/**"];

export default defineConfig({
  fmt: { ignorePatterns: ignore, printWidth: 110 },
  lint: { ignorePatterns: ignore, options: { typeAware: true, typeCheck: true } },
  test: { exclude: ["**/node_modules/**", "**/.artifacts/**"] },
});
