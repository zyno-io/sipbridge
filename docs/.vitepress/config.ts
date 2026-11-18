import { posix } from 'node:path'
import { defineConfig } from 'vitepress'

// Public documentation and source links use the project's GitHub repository.
const repository = 'https://github.com/zyno-io/sipbridge'
const sourceBase = `${repository}/blob/main`
const siteUrl = 'https://zyno-io.github.io/sipbridge/'
const ogImage = `${siteUrl}og-image.png`

// Root normative documents are rendered in place by include wrappers under
// spec/. Their repository paths stay authoritative and unchanged.
const includedSources: Record<string, string> = {
  'ARCHITECTURE.md': '/spec/architecture',
  'PROTOCOL.md': '/spec/protocol'
}

// Repository paths under docs/ that are not published as site pages.
const unpublished = ['docs/upstream']

// Resolve a relative Markdown link against its repository source file. Site
// pages become absolute site links; every other repository path becomes a
// source link, so links written for repository browsing also work on the site.
// Included content tries its original file first, then the wrapper page.
function siteHref(href: string, sourceFiles: string[]): string {
  if (/^[a-z][a-z0-9+.-]*:/i.test(href) || href.startsWith('#') || href.startsWith('/')) {
    return href
  }
  const [, target, suffix] = /^([^?#]*)(.*)$/.exec(href)!
  for (const sourceFile of sourceFiles) {
    const resolved = posix.normalize(posix.join(posix.dirname(sourceFile), target)).replace(/\/$/, '')
    if (resolved === '..' || resolved.startsWith('../')) {
      continue
    }
    if (includedSources[resolved]) {
      return includedSources[resolved] + suffix
    }
    if (resolved.startsWith('docs/') && !unpublished.some((path) => resolved === path || resolved.startsWith(`${path}/`))) {
      return `/${resolved.slice('docs/'.length)}${suffix}`
    }
    return `${sourceBase}/${resolved}${suffix}`
  }
  return href
}

export default defineConfig({
  title: 'sipbridge',
  description: 'A SIP signaling server that your application controls over a WebSocket',
  base: '/sipbridge/',
  srcExclude: ['upstream/**'],
  sitemap: { hostname: siteUrl },
  head: [
    ['link', { rel: 'icon', type: 'image/svg+xml', href: '/sipbridge/logo.svg' }],
    ['meta', { name: 'theme-color', content: '#5b63e8' }]
  ],
  // Link previews: Open Graph and Twitter card tags for every page.
  transformHead({ pageData, title, description }) {
    if (pageData.isNotFound) {
      return []
    }
    const path = pageData.relativePath.replace(/(^|\/)index\.md$/, '$1').replace(/\.md$/, '.html')
    return [
      ['meta', { property: 'og:type', content: 'website' }],
      ['meta', { property: 'og:site_name', content: 'sipbridge' }],
      ['meta', { property: 'og:title', content: title }],
      ['meta', { property: 'og:description', content: description }],
      ['meta', { property: 'og:url', content: siteUrl + path }],
      ['meta', { property: 'og:image', content: ogImage }],
      ['meta', { property: 'og:image:width', content: '1200' }],
      ['meta', { property: 'og:image:height', content: '630' }],
      ['meta', { property: 'og:image:alt', content: 'sipbridge: SIP signaling that your application controls' }],
      ['meta', { name: 'twitter:card', content: 'summary_large_image' }],
      ['meta', { name: 'twitter:image', content: ogImage }],
      ['meta', { name: 'twitter:title', content: title }],
      ['meta', { name: 'twitter:description', content: description }],
      ['link', { rel: 'canonical', href: siteUrl + path }]
    ]
  },
  markdown: {
    config(md) {
      md.core.ruler.push('sipbridge_source_links', (state) => {
        const env = state.env ?? {}
        const page = `docs/${env.relativePath ?? ''}`
        const sourceFiles: string[] = env.frontmatter?.sourcePath ? [env.frontmatter.sourcePath, page] : [page]
        const visit = (tokens: typeof state.tokens) => {
          for (const token of tokens) {
            if (token.type === 'link_open') {
              const href = token.attrGet('href')
              if (href) {
                token.attrSet('href', siteHref(href, sourceFiles))
              }
            }
            if (token.children) {
              visit(token.children)
            }
          }
        }
        visit(state.tokens)
      })
    }
  },
  themeConfig: {
    logo: '/logo.svg',
    search: { provider: 'local' },
    outline: { level: [2, 3] },
    nav: [
      { text: 'Guide', link: '/guide/overview' },
      { text: 'Protocol', link: '/protocol/overview' },
      { text: 'Reference', link: '/reference/limits' },
      { text: 'Advanced', link: '/spec/architecture' },
      { text: 'GitHub', link: repository }
    ],
    sidebar: [
      {
        text: 'Introduction',
        items: [
          { text: 'Overview', link: '/guide/overview' },
          { text: 'Getting Started', link: '/guide/getting-started' },
          { text: 'A Call, Step by Step', link: '/guide/call-walkthrough' }
        ]
      },
      {
        text: 'Building a Controller',
        items: [
          { text: 'Controller Duties', link: '/guide/application' },
          { text: 'Protocol Overview', link: '/protocol/overview' },
          { text: 'Session', link: '/protocol/session' },
          { text: 'Capabilities', link: '/protocol/capabilities' },
          { text: 'Message Families', link: '/protocol/messages' },
          { text: 'Commands and Outcomes', link: '/protocol/commands' },
          { text: 'Controller Restarts', link: '/guide/controller-recovery' }
        ]
      },
      {
        text: 'Running sipbridge',
        items: [
          { text: 'Configuration', link: '/guide/configuration' },
          { text: 'State Backends', link: '/guide/state-backends' },
          { text: 'Identities and TLS', link: '/guide/security' },
          { text: 'Health and Readiness', link: '/guide/health' },
          { text: 'Restarts and Disconnects', link: '/guide/lifecycle' },
          { text: 'Deployment', link: '/guide/deployment' },
          { text: 'Observability', link: '/guide/observability' },
          { text: 'Troubleshooting', link: '/guide/troubleshooting' }
        ]
      },
      {
        text: 'Reference',
        items: [
          { text: 'Limits and Timers', link: '/reference/limits' },
          { text: 'Admin Endpoints', link: '/reference/admin-endpoints' },
          { text: 'Glossary', link: '/reference/glossary' },
          { text: 'Project Status', link: '/reference/status' },
          { text: 'Development', link: '/guide/development' }
        ]
      },
      {
        text: 'Advanced: Specifications',
        collapsed: true,
        items: [
          { text: 'Architecture', link: '/spec/architecture' },
          { text: 'Control Protocol', link: '/spec/protocol' },
          { text: 'Service Runtime', link: '/runtime' },
          { text: 'Resident Memory Core', link: '/memory-core' },
          { text: 'Binary Redis Core', link: '/redis-core' },
          { text: 'Verification', link: '/verification' },
          { text: 'Redis Process Qualification', link: '/redis-process-qualification' }
        ]
      },
      {
        text: 'Advanced: Test Fixtures',
        collapsed: true,
        items: [
          { text: 'Fixtures', link: '/fixtures' },
          { text: 'Controller Recovery (fixture)', link: '/controller-recovery' },
          { text: 'Checkpoint Operation Flows', link: '/operations/' },
          { text: 'Execution and Storage', link: '/operations/execution' },
          { text: 'Scheduling and Lifecycle', link: '/operations/lifecycle' },
          { text: 'SIP Operations', link: '/operations/sip' }
        ]
      }
    ],
    socialLinks: [
      { icon: 'github', link: repository }
    ],
    footer: {
      message: 'sipbridge holds SIP connections and runs the protocol; your application decides what happens on each call.'
    }
  }
})
