import fs from 'node:fs';
import path from 'node:path';
import * as babel from '@babel/core';
import { generateTypes } from '@internal/types-builder';
import { defineConfig } from 'tsdown';

const pkg = JSON.parse(fs.readFileSync(path.join(import.meta.dirname, 'package.json'), 'utf-8'));
const { default: treeshakeDecoratorsBabelPlugin } = await import(
  new URL('./tools/treeshake-decorators.js', import.meta.url).href
);

/**
 * Node-only optional dependencies that must never appear as a literal specifier in a dynamic
 * `import()` in dist. Downstream bundlers (Vite/Rollup, esbuild's dep optimizer, wrangler)
 * follow literal specifiers even inside never-executed branches, which breaks builds of apps
 * that merely import `@mastra/core/agent`. `src/workspace/import-external.ts` keeps these
 * specifiers behind a function parameter so rolldown cannot constant-fold them; this check
 * fails the build if that ever stops working.
 *
 * Deliberately a deny-list rather than "any @vite-ignore'd literal": `src/channels/chat-lazy.ts`
 * needs its literal specifier so serverless bundlers *do* bundle the `chat` package (#19254).
 */
const OPAQUE_DYNAMIC_IMPORTS = ['execa', '@ast-grep/napi'];

/**
 * Matches `import(` followed by any interleaved comments (rolldown emits the `@vite-ignore` /
 * `webpackIgnore` hints on their own lines) and then a string-literal specifier.
 */
const LITERAL_DYNAMIC_IMPORT = /\bimport\(\s*(?:(?:\/\*[\s\S]*?\*\/|\/\/[^\n]*)\s*)*(['"])([^'"]+)\1/g;

/** True when the match starts on a comment line, e.g. a doc comment that mentions the pattern. */
function isInsideComment(code: string, index: number): boolean {
  const lineStart = code.lastIndexOf('\n', index) + 1;
  const linePrefix = code.slice(lineStart, index).trimStart();
  return linePrefix.startsWith('*') || linePrefix.startsWith('//') || linePrefix.startsWith('/*');
}

function collectBundleFiles(dir: string, out: string[] = []): string[] {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      collectBundleFiles(full, out);
    } else if (entry.name.endsWith('.js') || entry.name.endsWith('.cjs')) {
      out.push(full);
    }
  }
  return out;
}

function assertOpaqueDynamicImports(distDir: string) {
  if (!fs.existsSync(distDir)) {
    return;
  }

  const violations: string[] = [];
  for (const file of collectBundleFiles(distDir)) {
    const code = fs.readFileSync(file, 'utf-8');
    for (const match of code.matchAll(LITERAL_DYNAMIC_IMPORT)) {
      if (OPAQUE_DYNAMIC_IMPORTS.includes(match[2]!) && !isInsideComment(code, match.index)) {
        const line = code.slice(0, match.index).split('\n').length;
        violations.push(`${path.relative(process.cwd(), file)}:${line} -> import("${match[2]}")`);
      }
    }
  }

  if (violations.length > 0) {
    throw new Error(
      `Node-only dependencies leaked into dist as literal dynamic imports:\n` +
        violations.map(v => `  ${v}`).join('\n') +
        `\n\nThese specifiers must stay opaque to downstream bundlers. Import them through ` +
        `importExternal() from src/workspace/import-external.ts, and check that the bundler is ` +
        `not constant-folding the specifier into the import() call.`,
    );
  }

  console.info(`✓ No literal dynamic imports of ${OPAQUE_DYNAMIC_IMPORTS.join(', ')} in dist/`);
}

const treeshakeDecorators = {
  name: 'treeshake-decorators',
  renderChunk(code: string, chunk: { fileName: string }) {
    if (!code.includes('__decoratorStart')) {
      return null;
    }

    return new Promise((resolve, reject) => {
      babel.transform(
        code,
        {
          babelrc: false,
          configFile: false,
          filename: chunk.fileName,
          plugins: [treeshakeDecoratorsBabelPlugin],
        },
        (err, result) => {
          if (err) {
            return reject(err);
          }

          resolve({
            code: result!.code!,
            map: result!.map!,
          });
        },
      );
    });
  },
};

export default defineConfig({
  entry: [
    'src/index.ts',
    'src/base.ts',
    'src/utils.ts',
    '!src/action/index.ts',
    'src/*/index.ts',
    'src/observability/context-storage.ts',
    'src/tools/is-vercel-tool.ts',
    'src/workflows/constants.ts',
    'src/storage/constants.ts',
    'src/workflows/builder/index.ts',
    'src/workflows/evented/index.ts',
    'src/network/index.ts',
    'src/network/vNext/index.ts',
    'src/vector/filter/index.ts',
    'src/test-utils/llm-mock.ts',
    'src/a2a/client.ts',
    'src/processors/index.ts',
    'src/zod-to-json.ts',
    'src/utils/collect-tool-mocks.ts',
    'src/utils/safe-stringify.ts',
    'src/evals/scoreTraces/index.ts',
    'src/agent/message-list/index.ts',
    'src/agent/durable/index.ts',
    'src/auth/ee/index.ts',
    'src/auth/ee/fga-check.ts',
    'src/agent-builder/ee/index.ts',
    'src/storage/domains/agents/index.ts',
    'src/storage/domains/mcp-clients/index.ts',
    'src/storage/domains/mcp-servers/index.ts',
    'src/storage/domains/prompt-blocks/index.ts',
    'src/storage/domains/scorer-definitions/index.ts',
    'src/storage/domains/skills/index.ts',
    'src/storage/domains/favorites/index.ts',
    'src/storage/domains/workspaces/index.ts',
  ],
  format: ['esm', 'cjs'],
  fixedExtension: false,
  nodeProtocol: 'strip',
  clean: true,
  dts: false,
  treeshake: true,
  inputOptions: {
    plugins: [treeshakeDecorators],
  },
  define: {
    __MASTRA_VERSION__: JSON.stringify(pkg.version),
  },
  sourcemap: true,
  deps: {
    neverBundle: ['vite', 'vitest'],
    alwaysBundle: [
      '@ai-sdk/openai',
      '@internal/ai-sdk-v4',
      '@internal/ai-sdk-v5',
      '@internal/ai-v6',
      '@internal/auth',
      '@internal/core',
      '@internal/voice',
    ],
  },
  onSuccess: async () => {
    assertOpaqueDynamicImports(path.join(process.cwd(), 'dist'));

    await new Promise(resolve => setTimeout(resolve, 1000));
    await generateTypes(
      process.cwd(),
      new Set([
        '@ai-sdk/*',
        'eventsource-parser',
        '@internal/ai-sdk-v4',
        '@internal/ai-sdk-v5',
        '@internal/ai-v6',
        '@internal/ai-v7',
        '@internal/external-types',
        '@internal/core',
        '@internal/voice',
        'hono',
        'hono-openapi',
        '@internal/auth',
      ]),
    );

    // Copy provider-registry.json to dist folder
    const srcJson = path.join(process.cwd(), 'src/llm/model/provider-registry.json');
    const distJson = path.join(process.cwd(), 'dist/provider-registry.json');

    if (fs.existsSync(srcJson)) {
      fs.copyFileSync(srcJson, distJson);
      console.info('✓ Copied provider-registry.json to dist/');
    }

    // Copy capabilities/ directory to dist/
    const srcCapDir = path.join(process.cwd(), 'src/llm/model/capabilities');
    const distCapDir = path.join(process.cwd(), 'dist/capabilities');

    if (fs.existsSync(srcCapDir)) {
      if (!fs.existsSync(distCapDir)) {
        fs.mkdirSync(distCapDir, { recursive: true });
      }
      for (const file of fs.readdirSync(distCapDir).filter((f: string) => f.endsWith('.json'))) {
        fs.unlinkSync(path.join(distCapDir, file));
      }
      const capFiles = fs.readdirSync(srcCapDir).filter((f: string) => f.endsWith('.json'));
      for (const file of capFiles) {
        fs.copyFileSync(path.join(srcCapDir, file), path.join(distCapDir, file));
      }
      console.info(`✓ Copied ${capFiles.length} capability files to dist/capabilities/`);
    }

    // Copy provider-types.generated.d.ts to dist/llm/model/ folder
    const srcDts = path.join(process.cwd(), 'src/llm/model/provider-types.generated.d.ts');
    const distDtsDir = path.join(process.cwd(), 'dist/llm/model');
    const distDts = path.join(distDtsDir, 'provider-types.generated.d.ts');

    if (fs.existsSync(srcDts)) {
      // Ensure directory exists
      if (!fs.existsSync(distDtsDir)) {
        fs.mkdirSync(distDtsDir, { recursive: true });
      }
      fs.copyFileSync(srcDts, distDts);
      console.info('✓ Copied provider-types.generated.d.ts to dist/llm/model/');
    }
  },
});
