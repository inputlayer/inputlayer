const fs = require('fs')
const path = require('path')

const grammarPath = path.resolve(__dirname, '../grammars/iql.tmLanguage.json')

const withNextra = require('nextra')({
  theme: 'nextra-theme-docs',
  themeConfig: './theme.config.tsx',
  latex: true,
  mdxOptions: {
    rehypePrettyCodeOptions: {
      getHighlighter: async (options) => {
        const { getHighlighter } = await import('shiki')

        // Load custom IQL grammar
        const iqlGrammar = JSON.parse(fs.readFileSync(grammarPath, 'utf-8'))

        const highlighter = await getHighlighter({
          ...options,
          langs: [
            ...(options.langs || []),
            {
              id: 'iql',
              scopeName: 'source.iql',
              ...iqlGrammar,
            },
          ],
        })

        return highlighter
      },
    },
  },
})

module.exports = withNextra({
  output: 'export',
  images: { unoptimized: true },
})
