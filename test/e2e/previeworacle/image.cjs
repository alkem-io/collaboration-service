// Test-only Chromium oracle. Run with cwd in the disposable npm module.
const { copyFileSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } = require('node:fs');
const { createRequire } = require('node:module');
const { join, resolve } = require('node:path');

const scratch = process.cwd();
const load = createRequire(join(scratch, 'package.json'));
const { chromium } = load('playwright-core');
const fixtures = require('./images.json');
const svgFor = paint => '<svg xmlns="http://www.w3.org/2000/svg" width="64" height="48" viewBox="0 0 64 48">' + paint + '</svg>';

function xiaolaiCorpusManifest(dir) {
  // Use the exact @font-face range declarations, not local font discovery.
  const code = readFileSync(join(dir, 'Xiaolai/index.ts'), 'utf8');
  const paths = new Map([...code.matchAll(/import (_\d+) from "\.\/(.*\.woff2)";/g)].map(m => [m[1], m[2]]));
  const ranges = [...code.matchAll(/uri: (_\d+),\s*descriptors:\s*\{\s*unicodeRange:\s*"([^"]+)"/g)];
  if (ranges.length !== paths.size || paths.size !== 209) throw new Error('unexpected Xiaolai subset manifest');
  return ranges.map(([, id, range]) => ({ path: "Xiaolai/" + paths.get(id), range }));
}

async function main() {
  const root = mkdtempSync(join(scratch, 'whiteboard-oracle-'));
  const vite = await import(load.resolve('vite'));
  const server = await vite.createServer({ root, configFile: false,
    server: { host: '127.0.0.1', port: 0, strictPort: true, fs: { allow: [scratch] } } });
  try {
    copyFileSync(resolve(__dirname, 'image.html'), join(root, 'index.html'));
    symlinkSync(join(scratch, 'node_modules/@excalidraw-yjs/excalidraw/dist/prod'), join(root, 'editor-assets'), 'dir');
    await server.listen();
    const origin = new URL(server.resolvedUrls.local[0]).origin;
    const version = JSON.parse(readFileSync(join(scratch, 'node_modules/@excalidraw-yjs/excalidraw/package.json'))).version;
    if (version !== '0.5.6') throw new Error('wrong editor oracle: ' + version);
    const browser = await chromium.launch({ executablePath: process.env.CHROME_BIN, timeout: 8000 });
    try {
      const page = await browser.newPage();
      await page.route('**/*', r => r.request().url().startsWith(origin + '/') ? r.continue() : r.abort());
      page.on('pageerror', e => console.error(e));
      await page.goto(origin, { timeout: 12000 });
      await page.waitForFunction(() => Boolean(window.exportFixture), { timeout: 12000 });
      for (const { name, paint, center } of fixtures) {
        const svg = svgFor(paint);
        const result = await page.evaluate(svg => window.exportFixture(svg), svg);
        for (const image of result) {
          if (String(image.center) !== String(center)) throw new Error(JSON.stringify({ name, ...image }));
          console.log(JSON.stringify({ name, ...image }));
        }
      }
      const assets = svgFor(fixtures.find(({ name }) => name === 'local-pattern').paint);
      if (process.env.COLLAB_PREVIEW_MIXED_ORACLE) {
        const mixed = await page.evaluate(svg => window.exportMixedFixtures(svg), assets);
        writeFileSync(process.env.COLLAB_PREVIEW_MIXED_ORACLE, JSON.stringify(mixed), { flag: 'wx' });
        console.log('Captured', mixed.map(item => item.name).join(', '));
      }
      for (const [path, copies, mode] of [[process.env.COLLAB_PREVIEW_SAVED_ORACLE, 1, ""],
        [process.env.COLLAB_PREVIEW_SAVED_GRID_ORACLE, 64, ""], [process.env.COLLAB_PREVIEW_SAVED_TEXT_ORACLE, 1, "text"],
        [process.env.COLLAB_PREVIEW_SAVED_SHAPE_BURST_ORACLE, 625, "shape-burst"],
        [process.env.COLLAB_PREVIEW_SAVED_FRAME_ORACLE, 1, "frame"], [process.env.COLLAB_PREVIEW_SAVED_ARROW_ORACLE, 1, "arrow"],
        [process.env.COLLAB_PREVIEW_SAVED_FRAME_GROUP_ORACLE, 1, "frame-group"],
        [process.env.COLLAB_PREVIEW_SAVED_HEADS_ORACLE, 1, "heads"], [process.env.COLLAB_PREVIEW_SAVED_EMPTY_ORACLE, 1, "empty"],
        [process.env.COLLAB_PREVIEW_SAVED_CURVES_ORACLE, 1, "curves"],
        [process.env.COLLAB_PREVIEW_SAVED_PRESSURE_ORACLE, 1, "pressure"], [process.env.COLLAB_PREVIEW_SAVED_SCRIPTS_ORACLE, 1, "scripts"],
        [process.env.COLLAB_PREVIEW_SAVED_FONTS_ORACLE, 1, "fonts"],
        [process.env.COLLAB_PREVIEW_SAVED_CJK_ORACLE, 1, "cjk"],
        [process.env.COLLAB_PREVIEW_SAVED_TIED_ORACLE, 1, "tied"], [process.env.COLLAB_PREVIEW_SAVED_MISSING_INDEX_ORACLE, 1, "missing-index"],
        [process.env.COLLAB_PREVIEW_SAVED_CROP_ORACLE, 1, "crop"],
        [process.env.COLLAB_PREVIEW_SAVED_ROUNDED_ORACLE, 1, "rounded"],
        [process.env.COLLAB_PREVIEW_SAVED_STYLES_ORACLE, 1, "styles"],
        [process.env.COLLAB_PREVIEW_SAVED_EMBEDS_ORACLE, 1, "embeds"],
        [process.env.COLLAB_PREVIEW_SAVED_CAPTIONS_ORACLE, 1, "captions"],
        [process.env.COLLAB_PREVIEW_SAVED_ASSET_BURST_ORACLE, 1, "asset-burst"],
        [process.env.COLLAB_PREVIEW_SAVED_CODECS_ORACLE, 1, "codecs"]]) {
        if (!path) continue;
        const extras = [];
        if (mode === "codecs") {
          for (const [kind, asset] of [["bmp", process.env.COLLAB_PREVIEW_CODEC_BMP], ["avif", process.env.COLLAB_PREVIEW_CODEC_AVIF]]) {
            if (asset) {
              const mimeType = "image/" + kind;
              extras.push({mimeType, dataURL: "data:" + mimeType + ";base64," + readFileSync(asset).toString("base64")});
            }
          }
        }
        const saved = await page.evaluate(([svg, count, mode, extras]) => window.exportSavedFixture(svg, count, mode, extras),
          [assets, copies, mode, extras]);
        if (mode === "cjk") saved.fontSubsets = xiaolaiCorpusManifest(process.env.COLLAB_PREVIEW_FONTS_DIR);
        writeFileSync(path, JSON.stringify(saved), { flag: 'wx' });
        console.log('Captured saved Yjs source', saved.source, 'with', saved.order.length, 'live elements');
      }
    } finally {
      await browser.close();
    }
  } finally {
    await server.close();
    rmSync(root, { recursive: true });
  }
}

main().catch(error => { console.error(error); process.exitCode = 1; });
