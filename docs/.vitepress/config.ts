import { posix } from 'node:path'
import { defineConfig } from 'vitepress'

// Public documentation and source links use the project's GitHub repository.
const repository = 'https://github.com/zyno-io/sipbridge'
const sourceBase = `${repository}/blob/main`

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
    nav: [
      { text: 'Guide', link: '/guide/overview' },
      { text: 'Protocol', link: '/protocol/overview' },
      { text: 'Reference', link: '/reference/limits' },
      { text: 'Specification', link: '/spec/architecture' },
      { text: 'GitHub', link: repository }
    ],
    sidebar: [
      {
        text: 'Guide',
        items: [
          { text: 'Overview', link: '/guide/overview' },
          { text: 'Getting Started', link: '/guide/getting-started' },
          { text: 'Configuration', link: '/guide/configuration' },
          { text: 'State Backends', link: '/guide/state-backends' },
          { text: 'Identities and TLS', link: '/guide/security' },
          { text: 'Building a Controller', link: '/guide/application' },
          { text: 'Health and Readiness', link: '/guide/health' },
          { text: 'Lifecycle and Retention', link: '/guide/lifecycle' },
          { text: 'Controller Recovery', link: '/guide/controller-recovery' },
          { text: 'Deployment', link: '/guide/deployment' },
          { text: 'Troubleshooting', link: '/guide/troubleshooting' },
          { text: 'Observability', link: '/guide/observability' },
          { text: 'Development', link: '/guide/development' }
        ]
      },
      {
        text: 'Control Protocol',
        items: [
          { text: 'Overview', link: '/protocol/overview' },
          { text: 'Session', link: '/protocol/session' },
          { text: 'Capabilities', link: '/protocol/capabilities' },
          { text: 'Message Families', link: '/protocol/messages' },
          { text: 'Commands and Outcomes', link: '/protocol/commands' }
        ]
      },
      {
        text: 'Reference',
        items: [
          { text: 'Limits and Timers', link: '/reference/limits' },
          { text: 'Admin Endpoints', link: '/reference/admin-endpoints' },
          { text: 'Glossary', link: '/reference/glossary' },
          { text: 'Project Status', link: '/reference/status' }
        ]
      },
      {
        text: 'Specification',
        items: [
          { text: 'Architecture', link: '/spec/architecture' },
          { text: 'Control Protocol', link: '/spec/protocol' },
          { text: 'Service Runtime', link: '/runtime' },
          { text: 'Fixtures', link: '/fixtures' },
          { text: 'Verification', link: '/verification' },
          { text: 'Redis Process Qualification', link: '/redis-process-qualification' }
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
