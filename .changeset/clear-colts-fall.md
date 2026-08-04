---
'@mastra/core': patch
---

Fixed Vite and Cloudflare Workers builds failing on Mastra's optional Node-only dependencies.

Apps that import `@mastra/core/agent` pull in the workspace module, which lazily loads two Node-only packages (`@ast-grep/napi` and `execa`) only when workspace tooling actually runs. The published bundle inlined those module names directly into the `import()` calls, so bundlers tried to resolve them at build time even though the code path never executes in a Workers or browser target:

```
[vite]: Rollup failed to resolve import "@ast-grep/napi" from ".../@mastra/core/dist/workspace-*.js"
```

Both specifiers are now hidden from bundlers again, so a plain `vite build --ssr` succeeds without stub plugins, `build.rollupOptions.external` entries, or `optimizeDeps.exclude` workarounds. A build-time check keeps the specifiers from leaking back into future releases.

The imports behave exactly as before at runtime: they still resolve in Node when the packages are installed, and still fall back to the existing error paths when they are not.
