import { appendFile } from 'node:fs/promises';
import { execFileSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';

export function pagesMode(config, branch = 'main') {
  if (config.build_type === 'workflow') return 'workflow';
  if ((!config.build_type || config.build_type === 'legacy') && config.source?.branch === branch && config.source?.path === '/') return 'branch';
  throw new Error(`Unsupported Pages source: ${JSON.stringify({ build_type: config.build_type, source: config.source })}. Select GitHub Actions in Settings > Pages, or publish the ${branch} branch from /(root).`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const repository = process.env.GITHUB_REPOSITORY;
  if (!repository) throw new Error('GITHUB_REPOSITORY is required.');
  let config;
  try { config = JSON.parse(execFileSync('gh', ['api', `repos/${repository}/pages`], { encoding: 'utf8' })); }
  catch { throw new Error(`Cannot read Pages configuration. Enable Pages at https://github.com/${repository}/settings/pages.`); }
  const mode = pagesMode(config);
  await appendFile(process.env.GITHUB_OUTPUT, `mode=${mode}\npage_url=${config.html_url}\n`);
  console.log(mode === 'branch'
    ? 'Pages publishes main /(root). Root entry points forward to site/. Using the existing branch publisher without a competing artifact deployment.'
    : 'Pages uses GitHub Actions. Deploying the site/ artifact.');
}
