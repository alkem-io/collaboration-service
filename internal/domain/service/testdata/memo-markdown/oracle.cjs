// Test-only. Pass the pinned server source checkout and a server checkout with
// its pinned dependencies installed. Print snapshots and oracle outputs as JSON.
const { readFileSync } = require('node:fs');
const { createRequire } = require('node:module');
const { join, resolve } = require('node:path');
const assert = require('node:assert/strict');

const [source, installed = source] = process.argv.slice(2);
const dep = createRequire(resolve(installed, 'package.json'));
const ts = dep('typescript');
const Y = dep('yjs');
for (const [pkg, version] of [
  ['@tiptap/static-renderer', '3.6.1'], ['@tiptap/y-tiptap', '3.0.0'], ['yjs', '13.6.27'],
]) {
  const metadata = resolve(installed, 'node_modules', pkg, 'package.json');
  assert.equal(JSON.parse(readFileSync(metadata, 'utf8')).version, version);
}
const base = resolve(source, 'src/domain/common/memo/conversion');
const modules = new Map();
function load(name) {
  if (modules.has(name)) return modules.get(name).exports;
  const mod = { exports: {} };
  modules.set(name, mod);
  const code = ts.transpile(readFileSync(join(base, `${name}.ts`), 'utf8'), {
    esModuleInterop: true,
    module: ts.ModuleKind.CommonJS,
    target: ts.ScriptTarget.ES2022,
  });
  new Function('require', 'exports', 'module', code)(id => {
    const alias = '@domain/common/memo/conversion/';
    if (id.startsWith(alias)) return load(id.slice(alias.length));
    return id.startsWith('./') ? load(id.slice(2)) : dep(id);
  }, mod.exports, mod);
  return mod.exports;
}
const { yjsStateToMarkdown } = load('yjs.state.to.markdown');

function node(name, attributes = {}, ...children) {
  const element = new Y.XmlElement(name);
  for (const [key, value] of Object.entries(attributes)) element.setAttribute(key, value);
  if (children.length) element.insert(0, children);
  return element;
}
function text(...runs) {
  const value = new Y.XmlText();
  value.applyDelta(runs.map(run => typeof run === 'string' ? { insert: run } : run));
  return value;
}
const paragraph = (...children) => node('paragraph', {}, ...children);
const p = value => paragraph(text(value));
const mark = (insert, attributes) => ({ insert, attributes });
const li = (...children) => node('listItem', {}, ...children);
const cell = (...children) => node('tableCell', {}, ...children);
const row = (...children) => node('tableRow', {}, ...children);
const fixtures = [];
function capture(name, children, deleteLast = false) {
  const ydoc = new Y.Doc();
  ydoc.clientID = 1234567 + fixtures.length;
  const root = ydoc.getXmlFragment('default');
  root.insert(0, children);
  if (deleteLast) root.delete(children.length - 1, 1);
  const saved = Buffer.from(Y.encodeStateAsUpdateV2(ydoc));
  fixtures.push({ name, snapshot: saved.toString('base64'), markdown: yjsStateToMarkdown(saved) });
  ydoc.destroy();
}

capture('empty', [paragraph()]);
capture('cleared', [p('deleted')], true);
capture('marks-and-text', [
  node('heading', { level: 1 }, text('Memo 👋 café Λ')),
  paragraph(text('a | b, <value> ', mark('bold', { bold: {} }), ' ', mark('em', { italic: {} }), ' ',
    mark('formatted', { bold: {}, italic: {}, link: { href: 'https://example.invalid/docs' } }), ' ',
    mark('inline code', { code: {} }), ' ', mark('strike', { strike: {} }), ' ',
    mark('highlight', { highlight: {} })), node('hardBreak'), text('next line')),
  paragraph(), p('<br>'), p('Before'),
  node('horizontalRule'),
  node('heading', { level: 6 }, text('End')),
]);
capture('blocks-and-nesting', [
  node('blockquote', {}, p('First'), p('Second'), node('blockquote', {}, p('Nested quote'))),
  node('codeBlock', { language: 'go' }, text('package main\n\n// literal * markdown')),
  node('bulletList', {}, li(p('Parent'), node('orderedList', { start: 8 }, li(p('Inner'), p('More')), li(p('Next')))), li(p('Other'))),
  node('blockquote', {}, node('orderedList', { start: 5 }, li(p('Quoted one')), li(p('Quoted two')))),
]);
capture('tables-and-embeds', [
  node('table', {}, row(node('tableHeader', {}, p('A')), node('tableHeader', {}, p('B'))),
    row(cell(p('one'), p('two')), cell(paragraph(node('hardBreak')))), row(cell(p('<br>')), cell(paragraph()))),
  paragraph(node('image', { src: 'https://files.example.invalid/image.png', alt: 'image alt', title: 'not emitted' })),
  node('iframe', { src: 'https://example.invalid/movie' }),
  node('iframe', { src: 'https://example.invalid/second', width: '75%', height: '400', allowfullscreen: false }),
  p('deleted content'),
], true);

process.stdout.write(`${JSON.stringify(fixtures, null, 2)}\n`);
