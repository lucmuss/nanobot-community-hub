const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');

async function ensureVisible(page, selector) {
  await page.locator(selector).first().waitFor({ state: 'visible', timeout: 30000 });
}

async function captureTargets(page, baseUrl, outputDir, targets, viewport, suffix = '') {
  await page.setViewportSize(viewport);
  for (const [route, fileName, readySelector] of targets) {
    await page.goto(`${baseUrl}${route}`, { waitUntil: 'load' });
    await ensureVisible(page, readySelector);
    const outputName = suffix
      ? fileName.replace(/\.png$/i, `${suffix}.png`)
      : fileName;
    await page.screenshot({
      path: path.join(outputDir, outputName),
      fullPage: true,
    });
  }
}

async function captureDrawerState(page, baseUrl, outputDir, route, fileName) {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto(`${baseUrl}${route}`, { waitUntil: 'load' });
  await ensureVisible(page, '[data-testid="hub-mobile-menu-toggle"]');
  await page.getByTestId('hub-mobile-menu-toggle').click();
  await ensureVisible(page, '[data-testid="hub-mobile-drawer"]');
  await page.screenshot({
    path: path.join(outputDir, fileName),
    fullPage: true,
  });
}

async function main() {
  const baseUrl = process.env.NANOBOT_HUB_BASE_URL || 'http://127.0.0.1:18811';
  const outputDir =
    process.env.NANOBOT_HUB_SCREENSHOT_DIR || path.join(process.cwd(), 'output', 'gui-screenshots');
  const adminIdentifier = process.env.NANOBOT_HUB_ADMIN_IDENTIFIER || '';
  const adminPassword = process.env.NANOBOT_HUB_ADMIN_PASSWORD || '';

  const targets = [
    ['/discover', 'hub-discover.png', '[data-testid="hub-discover-search"]'],
    ['/mcp/context7', 'hub-mcp-detail-context7.png', '[data-testid="hub-mcp-detail-page"]'],
    ['/stacks', 'hub-stacks.png', '[data-testid="hub-stacks-page"]'],
    ['/stacks/github-developer-stack', 'hub-stack-detail-github-developer-stack.png', '[data-testid="hub-stack-detail-page"]'],
    ['/showcase', 'hub-showcase.png', '[data-testid="hub-showcase-page"]'],
    ['/showcase/ai-research-assistant', 'hub-showcase-detail-ai-research-assistant.png', '[data-testid="hub-showcase-detail-page"]'],
    ['/community-stats', 'hub-community-stats.png', '[data-testid="hub-community-stats-page"]'],
  ];
  const adminTargets = [
    ['/admin', 'hub-admin-dashboard.png', '[data-testid="hub-admin-runtime-settings-form"]'],
    ['/discover', 'hub-discover-admin.png', '[data-testid="hub-discover-search"]'],
    ['/stacks', 'hub-stacks-admin.png', '[data-testid="hub-stacks-page"]'],
    ['/showcase', 'hub-showcase-admin.png', '[data-testid="hub-showcase-page"]'],
  ];

  fs.mkdirSync(outputDir, { recursive: true });

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();

  await captureTargets(page, baseUrl, outputDir, targets, { width: 1440, height: 1080 });
  await captureTargets(page, baseUrl, outputDir, targets, { width: 390, height: 844 }, '-mobile');
  await captureDrawerState(page, baseUrl, outputDir, '/discover', 'hub-discover-drawer-mobile.png');

  if (adminIdentifier && adminPassword) {
    await page.goto(`${baseUrl}/login`, { waitUntil: 'load' });
    await ensureVisible(page, '[data-testid="hub-login-form"]');
    await page.getByTestId('hub-login-identifier').fill(adminIdentifier);
    await page.getByTestId('hub-login-password').fill(adminPassword);
    await page.getByTestId('hub-login-submit').click();
    await ensureVisible(page, '[data-testid="hub-admin-runtime-settings-form"]');
    await captureTargets(page, baseUrl, outputDir, adminTargets, { width: 1440, height: 1080 });
    await captureTargets(page, baseUrl, outputDir, adminTargets, { width: 390, height: 844 }, '-mobile');
    await captureDrawerState(page, baseUrl, outputDir, '/admin', 'hub-admin-drawer-mobile.png');
  }

  await browser.close();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
