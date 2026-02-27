import React from 'react'
import { DocsThemeConfig, useConfig } from 'nextra-theme-docs'

const config: DocsThemeConfig = {
  logo: (
    <>
      <svg
        xmlns="http://www.w3.org/2000/svg"
        width="24"
        height="24"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
      >
        <path d="M12 2L2 7l10 5 10-5-10-5z" />
        <path d="M2 17l10 5 10-5" />
        <path d="M2 12l10 5 10-5" />
      </svg>
      <span style={{ marginLeft: '.4em', fontWeight: 800 }}>InputLayer</span>
    </>
  ),
  head: function UseHead() {
    const { title } = useConfig()
    return (
      <>
        <title>{title ? `${title} | InputLayer Docs` : 'InputLayer Docs'}</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <meta
          property="og:title"
          content={title ? `${title} | InputLayer Docs` : 'InputLayer Docs'}
        />
        <meta
          property="og:description"
          content="Documentation for InputLayer — a symbolic reasoning engine for AI agents"
        />
      </>
    )
  },
  project: {
    link: 'https://github.com/InputLayer',
  },
  docsRepositoryBase:
    'https://github.com/InputLayer/FlowLog/tree/main/course/inputlayer/docs/content/',
  footer: {
    text: (
      <div>Copyright &copy; {new Date().getFullYear()} InputLayer</div>
    ),
  },
  search: {
    placeholder: 'Search documentation...',
  },
  sidebar: {
    defaultMenuCollapseLevel: 1,
    toggleButton: true,
  },
  toc: {
    backToTop: true,
  },
}

export default config
