const fs = require('fs')
const path = require('path')

const grammarPath = path.resolve(__dirname, '../grammars/datalog.tmLanguage.json')

const withNextra = require('nextra')({
  theme: 'nextra-theme-docs',
  themeConfig: './theme.config.tsx',
  latex: true,
  mdxOptions: {
    rehypePrettyCodeOptions: {
      getHighlighter: async (options) => {
        const { getHighlighter } = await import('shiki')

        // Load custom Datalog grammar
        const datalogGrammar = JSON.parse(fs.readFileSync(grammarPath, 'utf-8'))

        const highlighter = await getHighlighter({
          ...options,
          langs: [
            ...(options.langs || []),
            {
              id: 'datalog',
              scopeName: 'source.datalog',
              ...datalogGrammar,
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
