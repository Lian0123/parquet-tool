# Search and AI-search maintenance

The website targets `https://lian0123.github.io/parquet-tool/`. SEO work makes its
real content discoverable and understandable; it does not guarantee crawling,
rankings, rich results, or citations in generated answers.

## What is included

- Distinct titles/descriptions, one descriptive H1, HTTPS canonical URLs, and
  index/follow metadata on the home, converter, and complete-guide pages.
- Open Graph and Twitter summary metadata with a real 1200 × 630 PNG share card.
  `site/social-card.svg` is its editable source. The PNG is a committed static asset,
  not regenerated during normal builds. After changing the SVG, export a matching
  PNG and visually inspect it; the test checks its dimensions and existence.
- Static JSON-LD: WebSite/WebPage, converter WebApplication, breadcrumbs, visible
  FAQ questions/answers, and TechArticle/SoftwareSourceCode for the complete guide.
  No invented reviews, ratings, organization, or guaranteed rich-result claims.
- `guide.html` and `guide.md`: all twelve task descriptions, prerequisites, CLI and
  CommonJS snippets, outcomes, and source links. The HTML guide works with JavaScript
  disabled; interactive tab switching is no longer the only way to read examples.
- `sitemap.xml`: the three canonical HTML pages, excluding hash states, samples,
  generated downloads, and alternate representations. No artificial `lastmod` is
  emitted on every build.
- Optional `llms.txt`: a concise content/link index with the same support limits.
  It is a convenience file, not a crawler-access rule or required GEO signal.

Google says existing SEO fundamentals apply to AI features: make important text
available, link useful pages, allow crawling, and keep structured data consistent
with visible content. No special AI file or schema is required. See
[AI features and your website](https://developers.google.com/search/docs/appearance/ai-features)
and the [structured-data policies](https://developers.google.com/search/docs/appearance/structured-data/sd-policies).

## Editing content and canonical URLs

`tools/site-converter/seo.config.mjs` owns the canonical base URL, page metadata,
FAQ copy and content-review date. The date is a manual record of content review;
update it only when the material is actually checked. There is no fabricated
publication date or automatically refreshed review claim.

`tools/site-converter/seo.mjs` renders marked SEO/FAQ blocks inside the two hand-edited
pages and generates the guide, Markdown, sitemap and optional LLM index. Guide
snippets come directly from `site/examples.js`. Do not edit generated content by
hand; edit the config, generator, or recipes, then run:

```sh
npm run seo --prefix tools/site-converter
npm run seo:check --prefix tools/site-converter
npm test --prefix tools/site-converter
npm run docs:check
```

The ordinary browser build also regenerates these files. Pages checks their
freshness **before** building, so stale committed content fails deployment checks.
Use `seo.config.mjs` to change `baseUrl` if adopting a custom domain; regenerate all
outputs, update the share-card source if its displayed URL changes, and verify
canonicals, sitemap, JSON-LD and social URLs together. Only Traditional Chinese is
published, so no nonexistent language alternatives are advertised.

Content changes should keep these constraints explicit: browser/native engines
are separate, Parquet interoperability is limited, uploaded files are not sent to
servers, flat tables only, and the engine's real size/row/column/cell limits apply.
Tests compare advertised limits with `site/converter-shared.mjs`.

## GitHub Pages robots.txt scope

A crawler reads `https://lian0123.github.io/robots.txt`, **not**
`https://lian0123.github.io/parquet-tool/robots.txt`. This repository deliberately
does not publish a misleading project-level robots file. The controlling file
belongs to the `Lian0123/lian0123.github.io` root website; it was not changed here.
See Google's [robots location rules](https://developers.google.com/crawling/docs/robots-txt/create-robots-txt).

After checking any existing root policy, its owner can add a sitemap declaration:

```text
Sitemap: https://lian0123.github.io/parquet-tool/sitemap.xml
```

Keep existing restrictions intentional. If the root policy blocks `/parquet-tool/`,
fix it there; a nested file cannot override it. Do not alter AI training permissions
solely to enable search. This change introduces no training-specific bot policy.
The actual live root policy could not be retrieved during this task, so access is
not claimed as remotely verified.

## After deployment

1. Confirm all three canonical pages, the PNG, Markdown guide and sitemap return
   successful HTTP responses at the public project URL. Inspect deployed source,
   not only the JavaScript-rendered page.
2. In an owner-verified Google Search Console property, submit
   `https://lian0123.github.io/parquet-tool/sitemap.xml`. Inspect home, converter and
   guide URLs; check Google-selected canonicals, indexing status and robots access.
   Submit the sitemap in Bing Webmaster Tools as appropriate.
3. Validate JSON-LD with the [Schema.org validator](https://validator.schema.org/).
   The [Google Rich Results Test](https://search.google.com/test/rich-results) checks
   supported Google features; valid Schema.org markup alone does not imply a
   supported or displayed search enhancement.
4. Check mobile readability and real converter behavior, then inspect share-card
   rendering on target services. Compare impressions, relevant query clicks, and
   converter usage over time. AI-search citation behavior needs external observation;
   do not infer improvement from markup tests alone.

Search Console/Bing ownership verification, sitemap submission, live indexing,
production robots policy and external AI citations were not changed or verified.
No analytics or tracking script was added.
