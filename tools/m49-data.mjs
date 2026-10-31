import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';

export const sourceUrl = 'https://unstats.un.org/unsd/methodology/m49/overview/';
export const sha256 = bytes => createHash('sha256').update(bytes).digest('hex').toUpperCase();
export const jsonBytes = value => Buffer.from(JSON.stringify(value, null, 2) + '\n');
const sorted = entries => Object.fromEntries(entries.sort(([a], [b]) => a.localeCompare(b, 'en')));

// Parse only the fixed English download table; no page scripts or external dependencies.
export function parseOfficial(html) {
  const table = html.match(/<table\b[^>]*id\s*=\s*"downloadTableEN"[^>]*>([\s\S]*?)<\/table>/i);
  assert(table, 'Official M49 English table missing; review upstream format');
  const regions = new Map(), countries = new Map();
  const clean = text => text.replace(/<[^>]*>/g, '').replace(/&amp;/g, '&').replace(/&#39;|&apos;/g, "'")
    .replace(/&quot;/g, '"').replace(/&nbsp;|&#160;/g, ' ').trim();
  for (const row of table[1].replace(/<!--[\s\S]*?-->/g, '').matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi)) {
    const cells = [...row[1].matchAll(/<td\b[^>]*>([\s\S]*?)<\/td>/gi)].map(x => clean(x[1]));
    if (cells[0] !== '001') continue;
    assert(cells.length === 15, 'Unexpected M49 column count');
    for (const i of [0, 2, 4, 6]) {
      if (!cells[i]) continue;
      assert(/^[0-9]{3}$/.test(cells[i]) && cells[i + 1], 'Invalid official region');
      assert(!regions.has(cells[i]) || regions.get(cells[i]) === cells[i + 1], 'Conflicting official region');
      regions.set(cells[i], cells[i + 1]);
    }
    const country = cells[10];
    if (!country) continue;
    assert(/^[A-Z]{2}$/.test(country) && !countries.has(country), 'Invalid/duplicate ISO country code');
    countries.set(country, cells[6] || cells[4] || cells[2] || '001');
  }
  assert(countries.size >= 200, 'Official table appears incomplete');
  return { regions: sorted([...regions]), countries: sorted([...countries]) };
}

export function generate(source, policy) {
  assert.equal(policy.regions.length, 30, 'Policy must specify World and 29 selectable regions');
  const codes = policy.regions.map(r => r.code);
  assert.equal(new Set(codes).size, 30, 'Duplicate policy code');
  assert.deepEqual([...codes].sort(), Object.keys(source.regions).sort(), 'Official region set changed; review policy explicitly');
  assert.equal(new Set(policy.regions.map(r => r.abbreviation)).size, 30, 'Duplicate abbreviation');
  for (const region of policy.regions) {
    assert(/^[0-9]{3}$/.test(region.code) && /^[A-Z]{2,4}$/.test(region.abbreviation) && region.nameZh);
    assert.equal(region.name, source.regions[region.code], 'Region name changed; review policy');
  }
  const countries = { ...source.countries };
  for (const [country, region] of Object.entries(policy.countryOverrides)) {
    assert(/^[A-Z]{2}$/.test(country) && codes.includes(region));
    assert(!countries[country] || countries[country] === region, 'Compatibility override conflicts with official mapping');
    countries[country] = region;
  }
  for (const [country, region] of Object.entries(countries)) {
    assert(/^[A-Z]{2}$/.test(country) && codes.includes(region), 'Invalid country mapping');
  }
  return { regions: [...policy.regions].sort((a, b) => a.code.localeCompare(b.code)), countries: sorted(Object.entries(countries)) };
}
