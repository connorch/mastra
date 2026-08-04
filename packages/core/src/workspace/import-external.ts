/**
 * Imports a module through a specifier that bundlers must not resolve at build time.
 *
 * Workspace tooling depends on a few Node-only packages that are optional at runtime
 * (`execa`, `@ast-grep/napi`). They must stay invisible to bundlers: downstream apps that
 * import `@mastra/core/agent` pull in the workspace chunk, and a literal specifier makes
 * Rollup try to resolve a native addon (`@ast-grep/napi`) or makes esbuild's dependency
 * optimizer crawl into Node-only conditional exports (`execa` -> `npm-run-path` ->
 * `unicorn-magic`). Both fail the downstream build even though the code path never runs.
 *
 * Hiding the specifier behind a local variable is not enough: rolldown (the bundler behind
 * tsdown) constant-folds `const mod = 'execa'` and `'@ast-grep' + '/napi'` straight into the
 * `import()` call, so the published dist ends up with the literal anyway. A function
 * parameter cannot be folded, which keeps the specifier opaque in the built output.
 *
 * `packages/core/tsdown.config.ts` asserts after every build that neither specifier made it into
 * dist as a literal dynamic import, so this can't silently regress.
 *
 * Note this is deliberately the opposite of `src/channels/chat-lazy.ts`, where the specifier
 * must stay literal so that serverless bundlers *do* bundle the `chat` package.
 */
export function importExternal<T = unknown>(specifier: string): Promise<T> {
  return import(/* @vite-ignore */ /* webpackIgnore: true */ specifier) as Promise<T>;
}
