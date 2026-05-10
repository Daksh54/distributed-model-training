# Generated Wasm Kernel

Run this from the repository root:

```bash
npm run wasm:build
```

`wasm-pack` writes `distributed_ml_kernel.js` and the matching `.wasm` file into this directory. The browser worker attempts to load that generated module first and falls back to the JavaScript kernel when it is not present.
