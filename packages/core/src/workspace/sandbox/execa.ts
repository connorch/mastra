import type { execa as execaType } from 'execa';

import { importExternal } from '../import-external';

let cached: typeof execaType | undefined;
let loading: Promise<typeof execaType> | undefined;

/**
 * Lazily imports execa through {@link importExternal}, which keeps the module specifier
 * opaque to bundlers. This prevents bundlers (Vite/Rollup/esbuild) from resolving execa at
 * build time, which is necessary for Cloudflare Workers where execa's transitive deps
 * (npm-run-path -> unicorn-magic) use Node-only conditional exports.
 */
export async function getExeca(): Promise<typeof execaType> {
  if (cached) {
    return cached;
  }
  if (!loading) {
    loading = (async () => {
      try {
        const { execa } = await importExternal<typeof import('execa')>('execa');
        cached = execa;
        return execa;
      } catch (err) {
        throw new Error(
          'execa is required for local process execution but is not available in this environment. ' +
            'LocalProcessManager is not supported in Cloudflare Workers or other non-Node runtimes.',
          { cause: err },
        );
      }
    })();
  }
  return loading;
}
